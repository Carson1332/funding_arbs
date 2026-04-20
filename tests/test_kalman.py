"""Tests for Kalman filter hedge ratio estimation."""

import pytest
import numpy as np

from research.kalman_hedge import SimpleHedgeRatio


class TestSimpleHedgeRatio:
    """Test the fallback simple hedge ratio estimator."""

    def test_estimate_series(self):
        np.random.seed(42)
        n = 200
        spot = 50000 + np.cumsum(np.random.normal(0, 100, n))
        perp = spot + np.random.normal(0, 10, n)  # perp tracks spot closely

        hedge = SimpleHedgeRatio(window=30)
        ratios = hedge.estimate_series(spot, perp)

        assert len(ratios) == n
        # Hedge ratio should be close to 1.0 since perp ≈ spot
        assert abs(ratios[-1] - 1.0) < 0.5

    def test_constant_ratio(self):
        """When perp = 2 * spot, hedge ratio should converge to ~2."""
        n = 200
        spot = np.linspace(100, 200, n)
        perp = 2 * spot + np.random.normal(0, 0.1, n)

        hedge = SimpleHedgeRatio(window=50)
        ratios = hedge.estimate_series(spot, perp)

        # Should converge near 2.0
        assert abs(ratios[-1] - 2.0) < 0.5


class TestDynamicHedgeRatio:
    """Test Kalman filter hedge ratio (requires filterpy)."""

    def test_import(self):
        """Test that we can at least import the class."""
        from research.kalman_hedge import DynamicHedgeRatio, KalmanHedgeParams
        # If filterpy is installed, test full functionality
        try:
            hr = DynamicHedgeRatio(KalmanHedgeParams())
            ratio = hr.update(50000.0, 50010.0)
            assert isinstance(ratio, float)
        except ImportError:
            pytest.skip("filterpy not installed")

    def test_series_estimation(self):
        """Test series estimation with Kalman filter."""
        try:
            from research.kalman_hedge import DynamicHedgeRatio, KalmanHedgeParams
        except ImportError:
            pytest.skip("filterpy not installed")

        np.random.seed(42)
        n = 100
        spot = 50000 + np.cumsum(np.random.normal(0, 50, n))
        perp = spot + np.random.normal(0, 5, n)

        try:
            hr = DynamicHedgeRatio(KalmanHedgeParams(delta=1e-3))
            ratios = hr.estimate_series(spot, perp)
            assert len(ratios) == n
            assert abs(ratios[-1] - 1.0) < 0.5
        except ImportError:
            pytest.skip("filterpy not installed")
