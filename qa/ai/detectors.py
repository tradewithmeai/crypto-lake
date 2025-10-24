"""
Anomaly Detectors

Provides statistical and ML-based anomaly detection with proper hygiene:
- Safe NaN handling
- Feature scaling
- Fit on clean data only
- Emit features and rationale
"""

import warnings
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings('ignore', category=UserWarning)


class ZScoreDetector:
    """
    Z-score based anomaly detector.

    Detects outliers in spread, volume, and VWAP drift using rolling Z-scores.
    """

    def __init__(self, window: str = "1h", k: float = 5.0):
        """
        Initialize detector.

        Args:
            window: Rolling window size (e.g., "1h", "30min")
            k: Z-score threshold (default: 5.0)
        """
        self.window = window
        self.k = k
        self.scaler = StandardScaler()

    def detect(self, df: pd.DataFrame, clean_mask: Optional[pd.Series] = None) -> List[Dict[str, Any]]:
        """
        Detect Z-score anomalies.

        Args:
            df: DataFrame with OHLCV + bid/ask/spread data
            clean_mask: Boolean mask indicating clean rows (no schema violations)

        Returns:
            List of anomaly dictionaries
        """
        if df.empty:
            return []

        anomalies = []

        # Use clean data only for computing statistics
        if clean_mask is not None:
            df_clean = df[clean_mask].copy()
        else:
            df_clean = df.copy()

        if df_clean.empty:
            logger.warning("No clean data for Z-score detector")
            return []

        # Ensure timestamp is sorted
        df_clean = df_clean.sort_values("ts").copy()

        # Process per symbol
        for symbol in df_clean["symbol"].unique():
            symbol_df = df_clean[df_clean["symbol"] == symbol].copy()

            if len(symbol_df) < 10:
                continue  # Need minimum data for rolling stats

            # Compute features (skip if NaN)
            features = self._compute_features(symbol_df)

            # Detect anomalies per feature
            for feature_name, feature_series in features.items():
                # Skip if all NaN
                if feature_series.isna().all():
                    continue

                # Compute rolling mean and std
                rolling_mean = feature_series.rolling(self.window, min_periods=1).mean()
                rolling_std = feature_series.rolling(self.window, min_periods=1).std()

                # Compute Z-scores
                z_scores = (feature_series - rolling_mean) / (rolling_std + 1e-10)

                # Find outliers
                outliers = z_scores.abs() > self.k

                # Emit anomalies
                for idx in outliers[outliers].index:
                    row = symbol_df.loc[idx]
                    anomalies.append({
                        "symbol": symbol,
                        "ts": row["ts"].isoformat(),
                        "features": {
                            feature_name: float(feature_series.loc[idx]) if not pd.isna(feature_series.loc[idx]) else None,
                            "z_score": float(z_scores.loc[idx]) if not pd.isna(z_scores.loc[idx]) else None,
                        },
                        "detector": "ZSCORE_" + feature_name.upper(),
                        "label": "anomaly",
                        "rationale": f"{feature_name} Z-score {z_scores.loc[idx]:.2f} exceeds threshold {self.k}"
                    })

        logger.info(f"ZScoreDetector found {len(anomalies)} anomalies")
        return anomalies

    def _compute_features(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """Compute features for Z-score detection."""
        features = {}

        # Spread in basis points (skip if bid/ask missing)
        if "spread" in df.columns and "bid" in df.columns and "ask" in df.columns:
            valid_quotes = df["bid"].notna() & df["ask"].notna() & df["spread"].notna()
            if valid_quotes.any():
                mid = (df["bid"] + df["ask"]) / 2
                spread_bp = (df["spread"] / mid) * 10000
                features["spread_bp"] = spread_bp.where(valid_quotes, np.nan)

        # Volume (log scale to handle large variations)
        if "volume_base" in df.columns:
            valid_vol = df["volume_base"] > 0
            if valid_vol.any():
                features["log_volume"] = np.log1p(df["volume_base"]).where(valid_vol, np.nan)

        # VWAP drift in basis points
        if "vwap" in df.columns and "close" in df.columns:
            valid_vwap = df["vwap"].notna() & df["close"].notna() & (df["close"] > 0)
            if valid_vwap.any():
                vwap_drift_bp = ((df["vwap"] - df["close"]) / df["close"]) * 10000
                features["vwap_drift_bp"] = vwap_drift_bp.where(valid_vwap, np.nan)

        return features


class JumpDetector:
    """
    Jump detector for sudden price movements.

    Detects large price jumps that exceed k_sigma standard deviations
    in stable spread conditions.
    """

    def __init__(self, k_sigma: float = 6.0, spread_stable_bps: float = 50, min_trade_count: int = 0):
        """
        Initialize detector.

        Args:
            k_sigma: Jump threshold in standard deviations (default: 6.0)
            spread_stable_bps: Maximum spread in bps for "stable" condition (default: 50)
            min_trade_count: Minimum trade count to consider (default: 0 = no filtering)
        """
        self.k_sigma = k_sigma
        self.spread_stable_bps = spread_stable_bps
        self.min_trade_count = min_trade_count

    def detect(self, df: pd.DataFrame, clean_mask: Optional[pd.Series] = None) -> List[Dict[str, Any]]:
        """
        Detect price jumps.

        Args:
            df: DataFrame with OHLCV + bid/ask/spread data
            clean_mask: Boolean mask indicating clean rows

        Returns:
            List of anomaly dictionaries
        """
        if df.empty:
            return []

        anomalies = []

        # Use clean data for computing statistics
        if clean_mask is not None:
            df_clean = df[clean_mask].copy()
        else:
            df_clean = df.copy()

        if df_clean.empty:
            logger.warning("No clean data for Jump detector")
            return []

        df_clean = df_clean.sort_values("ts").copy()

        # Process per symbol
        for symbol in df_clean["symbol"].unique():
            symbol_df = df_clean[df_clean["symbol"] == symbol].copy()

            if len(symbol_df) < 10:
                continue

            # Compute log returns
            symbol_df["log_ret"] = np.log(symbol_df["close"] / symbol_df["close"].shift(1))

            # Compute spread in bps (skip if missing)
            has_spread = ("spread" in symbol_df.columns and
                          "bid" in symbol_df.columns and
                          "ask" in symbol_df.columns)

            if has_spread:
                valid_quotes = symbol_df["bid"].notna() & symbol_df["ask"].notna()
                mid = (symbol_df["bid"] + symbol_df["ask"]) / 2
                symbol_df["spread_bp"] = ((symbol_df["spread"] / mid) * 10000).where(valid_quotes, np.nan)
            else:
                symbol_df["spread_bp"] = np.nan

            # Filter to stable spread conditions
            stable_mask = symbol_df["spread_bp"] <= self.spread_stable_bps
            if has_spread and stable_mask.sum() > 0:
                stable_returns = symbol_df.loc[stable_mask, "log_ret"].dropna()
            else:
                # No spread data or no stable periods - use all returns
                stable_returns = symbol_df["log_ret"].dropna()

            if len(stable_returns) < 10:
                continue

            # Compute mean and std from stable returns
            ret_mean = stable_returns.mean()
            ret_std = stable_returns.std()

            if ret_std == 0 or np.isnan(ret_std):
                continue

            # Find jumps
            z_scores = (symbol_df["log_ret"] - ret_mean) / ret_std
            jumps = z_scores.abs() > self.k_sigma

            # Emit anomalies
            for idx in jumps[jumps].index:
                row = symbol_df.loc[idx]
                if pd.isna(row["log_ret"]):
                    continue

                # Gate on trade_count if configured and column exists
                if self.min_trade_count > 0 and "trade_count" in row.index:
                    if pd.notna(row["trade_count"]) and row["trade_count"] < self.min_trade_count:
                        continue  # Skip ultra-thin seconds

                anomalies.append({
                    "symbol": symbol,
                    "ts": row["ts"].isoformat(),
                    "features": {
                        "log_return": float(row["log_ret"]),
                        "z_score": float(z_scores.loc[idx]),
                        "spread_bp": float(row["spread_bp"]) if not pd.isna(row["spread_bp"]) else None,
                    },
                    "detector": "JUMP",
                    "label": "anomaly",
                    "rationale": f"Price jump Z-score {z_scores.loc[idx]:.2f} exceeds {self.k_sigma}"
                })

        logger.info(f"JumpDetector found {len(anomalies)} anomalies")
        return anomalies


class IsolationForestDetector:
    """
    Isolation Forest based anomaly detector.

    Detects multivariate anomalies in feature space.
    """

    def __init__(self, n_estimators: int = 200, contamination: float = 0.005, random_state: int = 42):
        """
        Initialize detector.

        Args:
            n_estimators: Number of trees (default: 200)
            contamination: Expected anomaly proportion (default: 0.005)
            random_state: Random seed (default: 42)
        """
        self.n_estimators = n_estimators
        self.contamination = contamination
        self.random_state = random_state
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = []

    def detect(self, df: pd.DataFrame, clean_mask: Optional[pd.Series] = None) -> List[Dict[str, Any]]:
        """
        Detect anomalies using Isolation Forest.

        Args:
            df: DataFrame with OHLCV + bid/ask/spread data
            clean_mask: Boolean mask indicating clean rows (used for fitting)

        Returns:
            List of anomaly dictionaries
        """
        if df.empty:
            return []

        anomalies = []

        # Compute features for all rows
        feature_df, feature_matrix = self._compute_features(df)

        if feature_matrix is None or len(feature_matrix) == 0:
            logger.warning("No valid features for IsolationForest")
            return []

        # Fit model on CLEAN data only
        if clean_mask is not None:
            clean_features = feature_matrix[clean_mask]
        else:
            clean_features = feature_matrix

        if len(clean_features) < 10:
            logger.warning("Insufficient clean data for IsolationForest training")
            return []

        # Scale features
        self.scaler.fit(clean_features)
        scaled_features = self.scaler.transform(feature_matrix)

        # Fit Isolation Forest
        self.model = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            random_state=self.random_state,
            n_jobs=-1
        )
        self.model.fit(scaled_features[clean_mask] if clean_mask is not None else scaled_features)

        # Predict on all data
        predictions = self.model.predict(scaled_features)
        scores = self.model.score_samples(scaled_features)

        # Find anomalies (-1 = anomaly, 1 = normal)
        anomaly_mask = predictions == -1

        # Emit anomalies
        for idx in feature_df[anomaly_mask].index:
            row = df.loc[idx]
            features_dict = feature_df.loc[idx].to_dict()

            # Remove NaNs from features
            features_dict = {k: (float(v) if not pd.isna(v) else None) for k, v in features_dict.items()}

            anomalies.append({
                "symbol": row["symbol"],
                "ts": row["ts"].isoformat(),
                "features": features_dict,
                "detector": "IFOREST",
                "label": "anomaly",
                "rationale": f"IsolationForest anomaly score {scores[idx]:.4f}"
            })

        logger.info(f"IsolationForestDetector found {len(anomalies)} anomalies")
        return anomalies

    def _compute_features(self, df: pd.DataFrame) -> tuple:
        """
        Compute feature matrix for Isolation Forest.

        Returns:
            (feature_df, feature_matrix) tuple
        """
        features = {}

        # Log returns
        df_sorted = df.sort_values(["symbol", "ts"]).copy()
        df_sorted["ret1s"] = df_sorted.groupby("symbol")["close"].pct_change().fillna(0)
        features["ret1s"] = df_sorted["ret1s"]

        # Spread in bps (skip if missing)
        if "spread" in df.columns and "bid" in df.columns and "ask" in df.columns:
            valid_quotes = df_sorted["bid"].notna() & df_sorted["ask"].notna() & df_sorted["spread"].notna()
            mid = (df_sorted["bid"] + df_sorted["ask"]) / 2
            spread_bp = (df_sorted["spread"] / mid) * 10000
            features["spread_bp"] = spread_bp.where(valid_quotes, np.nan)
        else:
            features["spread_bp"] = np.nan

        # VWAP drift in bps
        if "vwap" in df.columns and "close" in df.columns:
            valid_vwap = df_sorted["vwap"].notna() & df_sorted["close"].notna() & (df_sorted["close"] > 0)
            vwap_drift = ((df_sorted["vwap"] - df_sorted["close"]) / df_sorted["close"]) * 10000
            features["vwap_drift_bp"] = vwap_drift.where(valid_vwap, np.nan)
        else:
            features["vwap_drift_bp"] = np.nan

        # Trade count (normalized per symbol)
        if "trade_count" in df.columns:
            features["log_trade_count"] = np.log1p(df_sorted["trade_count"])
        else:
            features["log_trade_count"] = 0

        # Create feature DataFrame
        feature_df = pd.DataFrame(features, index=df_sorted.index)

        # Drop rows with any NaN
        feature_df_clean = feature_df.dropna()

        if feature_df_clean.empty:
            return feature_df, None

        self.feature_names = list(feature_df_clean.columns)
        return feature_df, feature_df_clean.values
