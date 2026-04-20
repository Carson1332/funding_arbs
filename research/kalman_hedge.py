"""
Kalman Filter Dynamic Hedge Ratio Estimation.

Uses a linear state-space model to estimate the time-varying hedge ratio
between spot and perpetual prices. The hedge ratio is treated as a hidden
state that follows a random walk, estimated with noisy price observations.

This replaces the naive 1:1 hedge assumption used in simpler funding arb
implementations. The Kalman filter adapts to basis drift, providing a
more accurate hedge that reduces P&L variance from price movements.

Reference:
    "A sophisticated approach is to utilise a state space model that treats
    the 'true' hedge ratio as an unobserved hidden variable and attempts
    to estimate it with 'noisy' observations — the pricing data of each asset."
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

try:
    from filterpy.kalman import KalmanFilter
except ImportError:
    KalmanFilter = None


@dataclass
class KalmanHedgeParams:
    """Parameters for Kalman filter hedge ratio estimation."""

    delta: float = 1e-4  # process noise scaling (higher = more responsive)
    R: float = 1.0  # measurement noise (higher = smoother)
    initial_hedge_ratio: float = 1.0  # starting hedge ratio
    rebalance_threshold: float = 0.03  # 3% drift triggers rebalance


class DynamicHedgeRatio:
    """
    Kalman filter to estimate time-varying hedge ratio between spot and perp.

    State vector: [intercept, hedge_ratio]
    Measurement model: perp_price = intercept + hedge_ratio * spot_price + noise
    Transition model: random walk (state evolves slowly over time)
    """

    def __init__(self, params: Optional[KalmanHedgeParams] = None):
        self.params = params or KalmanHedgeParams()

        if KalmanFilter is None:
            raise ImportError(
                "filterpy is required for Kalman hedge ratio estimation. "
                "Install with: pip install filterpy"
            )

        self.kf = KalmanFilter(dim_x=2, dim_z=1)
        self.kf.x = np.array(
            [[0.0], [self.params.initial_hedge_ratio]]
        )  # [intercept, hedge_ratio]
        self.kf.P *= 1.0  # initial state covariance
        self.kf.Q = np.eye(2) * self.params.delta  # process noise
        self.kf.R = np.array([[self.params.R]])  # measurement noise
        self.kf.F = np.eye(2)  # state transition (random walk)

    def update(self, spot_price: float, perp_price: float) -> float:
        """
        Feed one observation, return updated hedge ratio.

        Parameters
        ----------
        spot_price : float
        perp_price : float

        Returns
        -------
        float : current hedge ratio estimate
        """
        self.kf.H = np.array([[1.0, spot_price]])  # measurement model
        self.kf.predict()
        self.kf.update(np.array([[perp_price]]))
        return self.kf.x[1, 0]  # hedge ratio β

    def estimate_series(
        self, spot_prices: np.ndarray, perp_prices: np.ndarray
    ) -> np.ndarray:
        """
        Run Kalman filter over full price series.

        Parameters
        ----------
        spot_prices : np.ndarray
        perp_prices : np.ndarray

        Returns
        -------
        np.ndarray : array of hedge ratios at each time step
        """
        n = len(spot_prices)
        ratios = np.zeros(n)
        intercepts = np.zeros(n)
        covariances = np.zeros(n)

        for i in range(n):
            ratios[i] = self.update(spot_prices[i], perp_prices[i])
            intercepts[i] = self.kf.x[0, 0]
            covariances[i] = self.kf.P[1, 1]

        return ratios

    def estimate_from_df(
        self, spot_df: pd.DataFrame, perp_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Estimate hedge ratios from spot and perp DataFrames.

        Parameters
        ----------
        spot_df : pd.DataFrame with columns [timestamp, close]
        perp_df : pd.DataFrame with columns [timestamp, close]

        Returns
        -------
        pd.DataFrame with columns:
            timestamp, spot_price, perp_price, hedge_ratio, intercept, spread
        """
        # Align on timestamp
        spot = spot_df[["timestamp", "close"]].rename(columns={"close": "spot_price"})
        perp = perp_df[["timestamp", "close"]].rename(columns={"close": "perp_price"})

        merged = pd.merge_asof(
            perp.sort_values("timestamp"),
            spot.sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta("1h"),
        ).dropna()

        if merged.empty:
            return pd.DataFrame()

        # Reset Kalman filter
        self.__init__(self.params)

        ratios = np.zeros(len(merged))
        intercepts = np.zeros(len(merged))

        for i in range(len(merged)):
            spot_p = merged.iloc[i]["spot_price"]
            perp_p = merged.iloc[i]["perp_price"]
            ratios[i] = self.update(spot_p, perp_p)
            intercepts[i] = self.kf.x[0, 0]

        merged["hedge_ratio"] = ratios
        merged["intercept"] = intercepts
        merged["spread"] = (
            merged["perp_price"]
            - merged["intercept"]
            - merged["hedge_ratio"] * merged["spot_price"]
        )

        return merged

    @property
    def current_hedge_ratio(self) -> float:
        """Return the current hedge ratio estimate."""
        return self.kf.x[1, 0]

    @property
    def current_uncertainty(self) -> float:
        """Return the current uncertainty (std dev) of the hedge ratio."""
        return np.sqrt(self.kf.P[1, 1])


class SimpleHedgeRatio:
    """
    Fallback: simple rolling OLS hedge ratio (no filterpy dependency).
    """

    def __init__(self, window: int = 90):
        self.window = window

    def estimate_series(
        self, spot_prices: np.ndarray, perp_prices: np.ndarray
    ) -> np.ndarray:
        """Rolling OLS hedge ratio."""
        n = len(spot_prices)
        ratios = np.ones(n)

        for i in range(self.window, n):
            x = spot_prices[i - self.window : i]
            y = perp_prices[i - self.window : i]
            # OLS: β = cov(x,y) / var(x)
            cov = np.cov(x, y)[0, 1]
            var = np.var(x)
            if var > 0:
                ratios[i] = cov / var
            else:
                ratios[i] = ratios[i - 1] if i > 0 else 1.0

        # Fill initial values
        ratios[:self.window] = ratios[self.window] if n > self.window else 1.0
        return ratios
