#!/usr/bin/env python3
"""
Machine Learning Pipeline - Enterprise ML Operations and Model Management
Advanced MLOps platform for model training, deployment, and monitoring.

Use of this code is at your own risk.
Author bears no responsibility for any damages caused by the code.
"""

import os
import sys
import json
import time
import pickle
import joblib
import asyncio
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
import hashlib
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading

# ML Libraries
try:
    import sklearn
    from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
    from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.svm import SVC, SVR
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, mean_squared_error, r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import tensorflow as tf
    from tensorflow import keras
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    PYTORCH_AVAILABLE = True
except ImportError:
    PYTORCH_AVAILABLE = False

class ModelType(Enum):
    """Machine learning model types."""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    DEEP_LEARNING = "deep_learning"
    NATURAL_LANGUAGE = "natural_language"
    COMPUTER_VISION = "computer_vision"
    TIME_SERIES = "time_series"
    REINFORCEMENT_LEARNING = "reinforcement_learning"

class ModelStatus(Enum):
    """Model lifecycle status."""
    TRAINING = "training"
    TRAINED = "trained"
    VALIDATING = "validating"
    DEPLOYED = "deployed"
    MONITORING = "monitoring"
    DEPRECATED = "deprecated"
    FAILED = "failed"

class DataType(Enum):
    """Data types for ML pipeline."""
    TABULAR = "tabular"
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    TIME_SERIES = "time_series"

@dataclass
class DatasetInfo:
    """Dataset information and metadata."""
    id: str
    name: str
    description: str
    data_type: DataType
    file_path: str
    size_mb: float
    num_samples: int
    num_features: int
    target_column: Optional[str] = None
    feature_columns: List[str] = field(default_factory=list)
    categorical_columns: List[str] = field(default_factory=list)
    numerical_columns: List[str] = field(default_factory=list)
    missing_values: Dict[str, int] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class ModelConfig:
    """Model configuration and hyperparameters."""
    model_id: str
    name: str
    model_type: ModelType
    algorithm: str
    hyperparameters: Dict[str, Any]
    preprocessing_steps: List[str] = field(default_factory=list)
    feature_selection: Optional[str] = None
    cross_validation_folds: int = 5
    test_size: float = 0.2
    random_state: int = 42
    created_by: str = "system"

@dataclass
class TrainingJob:
    """ML model training job."""
    job_id: str
    model_config: ModelConfig
    dataset_id: str
    status: ModelStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    model_path: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    resource_usage: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ModelMetrics:
    """Model performance metrics."""
    model_id: str
    dataset_id: str
    metrics: Dict[str, float]
    confusion_matrix: Optional[List[List[int]]] = None
    feature_importance: Dict[str, float] = field(default_factory=dict)
    training_time_seconds: float = 0.0
    inference_time_ms: float = 0.0
    model_size_mb: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class DeploymentConfig:
    """Model deployment configuration."""
    deployment_id: str
    model_id: str
    environment: str
    endpoint_url: str
    scaling_config: Dict[str, Any]
    monitoring_config: Dict[str, Any]
    rollback_config: Dict[str, Any] = field(default_factory=dict)
    deployed_at: Optional[datetime] = None
    status: str = "pending"

class DataProcessor:
    """Data preprocessing and feature engineering."""
    
    def __init__(self):
        self.logger = logging.getLogger('DataProcessor')
        self.scalers = {}
        self.encoders = {}
        
    def load_dataset(self, file_path: str, data_type: DataType) -> Tuple[pd.DataFrame, DatasetInfo]:
        """Load dataset from file."""
        try:
            if data_type == DataType.TABULAR:
                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path)
                elif file_path.endswith('.json'):
                    df = pd.read_json(file_path)
                elif file_path.endswith('.parquet'):
                    df = pd.read_parquet(file_path)
                else:
                    raise ValueError(f"Unsupported file format: {file_path}")
                
                # Generate dataset info
                dataset_info = self._analyze_dataset(df, file_path, data_type)
                
                self.logger.info(f"Loaded dataset: {dataset_info.num_samples} samples, {dataset_info.num_features} features")
                
                return df, dataset_info
            
            else:
                raise NotImplementedError(f"Data type {data_type.value} not implemented")
                
        except Exception as e:
            self.logger.error(f"Failed to load dataset {file_path}: {e}")
            raise
    
    def _analyze_dataset(self, df: pd.DataFrame, file_path: str, data_type: DataType) -> DatasetInfo:
        """Analyze dataset and generate metadata."""
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        
        # Identify column types
        numerical_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        # Count missing values
        missing_values = df.isnull().sum().to_dict()
        missing_values = {k: int(v) for k, v in missing_values.items() if v > 0}
        
        dataset_info = DatasetInfo(
            id=hashlib.md5(file_path.encode()).hexdigest()[:8],
            name=Path(file_path).stem,
            description=f"Dataset loaded from {file_path}",
            data_type=data_type,
            file_path=file_path,
            size_mb=file_size,
            num_samples=len(df),
            num_features=len(df.columns),
            feature_columns=df.columns.tolist(),
            categorical_columns=categorical_columns,
            numerical_columns=numerical_columns,
            missing_values=missing_values
        )
        
        return dataset_info
    
    def preprocess_data(self, df: pd.DataFrame, dataset_info: DatasetInfo, 
                       target_column: str = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Preprocess data for ML training."""
        try:
            processed_df = df.copy()
            preprocessing_info = {}
            
            # Handle missing values
            if dataset_info.missing_values:
                self.logger.info("Handling missing values...")
                
                for column, missing_count in dataset_info.missing_values.items():
                    if column in dataset_info.numerical_columns:
                        # Fill numerical columns with median
                        median_value = processed_df[column].median()
                        processed_df[column].fillna(median_value, inplace=True)
                        preprocessing_info[f"{column}_fill_value"] = median_value
                    
                    elif column in dataset_info.categorical_columns:
                        # Fill categorical columns with mode
                        mode_value = processed_df[column].mode().iloc[0] if not processed_df[column].mode().empty else "unknown"
                        processed_df[column].fillna(mode_value, inplace=True)
                        preprocessing_info[f"{column}_fill_value"] = mode_value
            
            # Encode categorical variables
            if dataset_info.categorical_columns:
                self.logger.info("Encoding categorical variables...")
                
                for column in dataset_info.categorical_columns:
                    if column != target_column:  # Don't encode target column yet
                        if processed_df[column].nunique() <= 10:
                            # One-hot encoding for low cardinality
                            encoder = OneHotEncoder(sparse_output=False, drop='first')
                            encoded_cols = encoder.fit_transform(processed_df[[column]])
                            
                            # Create column names
                            feature_names = [f"{column}_{cat}" for cat in encoder.categories_[0][1:]]
                            encoded_df = pd.DataFrame(encoded_cols, columns=feature_names, index=processed_df.index)
                            
                            # Replace original column
                            processed_df = processed_df.drop(columns=[column])
                            processed_df = pd.concat([processed_df, encoded_df], axis=1)
                            
                            self.encoders[column] = encoder
                        
                        else:
                            # Label encoding for high cardinality
                            encoder = LabelEncoder()
                            processed_df[column] = encoder.fit_transform(processed_df[column])
                            self.encoders[column] = encoder
            
            # Scale numerical features
            if dataset_info.numerical_columns:
                self.logger.info("Scaling numerical features...")
                
                numerical_cols = [col for col in dataset_info.numerical_columns if col != target_column]
                if numerical_cols:
                    scaler = StandardScaler()
                    processed_df[numerical_cols] = scaler.fit_transform(processed_df[numerical_cols])
                    self.scalers['numerical'] = scaler
            
            # Encode target column if categorical
            if target_column and target_column in dataset_info.categorical_columns:
                target_encoder = LabelEncoder()
                processed_df[target_column] = target_encoder.fit_transform(processed_df[target_column])
                self.encoders[f"{target_column}_target"] = target_encoder
            
            preprocessing_info['encoders'] = list(self.encoders.keys())
            preprocessing_info['scalers'] = list(self.scalers.keys())
            
            self.logger.info(f"Preprocessing completed. Shape: {processed_df.shape}")
            
            return processed_df, preprocessing_info
            
        except Exception as e:
            self.logger.error(f"Data preprocessing failed: {e}")
            raise
    
    def feature_selection(self, X: pd.DataFrame, y: pd.Series, method: str = "correlation", k: int = 10) -> List[str]:
        """Select top k features using specified method."""
        try:
            if method == "correlation":
                # Correlation-based feature selection
                correlations = X.corrwith(y).abs().sort_values(ascending=False)
                selected_features = correlations.head(k).index.tolist()
            
            elif method == "mutual_info" and SKLEARN_AVAILABLE:
                from sklearn.feature_selection import mutual_info_regression, mutual_info_classif
                
                if y.dtype in ['int64', 'int32'] and y.nunique() < 20:
                    # Classification
                    scores = mutual_info_classif(X, y)
                else:
                    # Regression
                    scores = mutual_info_regression(X, y)
                
                feature_scores = pd.Series(scores, index=X.columns).sort_values(ascending=False)
                selected_features = feature_scores.head(k).index.tolist()
            
            else:
                # Default: select first k features
                selected_features = X.columns[:k].tolist()
            
            self.logger.info(f"Selected {len(selected_features)} features using {method}")
            
            return selected_features
            
        except Exception as e:
            self.logger.error(f"Feature selection failed: {e}")
            return X.columns.tolist()

class ModelTrainer:
    """Machine learning model trainer."""
    
    def __init__(self):
        self.logger = logging.getLogger('ModelTrainer')
        self.models = {}
        
    def create_model(self, config: ModelConfig) -> Any:
        """Create ML model based on configuration."""
        try:
            if not SKLEARN_AVAILABLE:
                raise ImportError("scikit-learn not available")
            
            algorithm = config.algorithm.lower()
            params = config.hyperparameters
            
            if config.model_type == ModelType.CLASSIFICATION:
                if algorithm == "random_forest":
                    model = RandomForestClassifier(**params)
                elif algorithm == "logistic_regression":
                    model = LogisticRegression(**params)
                elif algorithm == "svm":
                    model = SVC(**params)
                elif algorithm == "mlp":
                    model = MLPClassifier(**params)
                else:
                    raise ValueError(f"Unknown classification algorithm: {algorithm}")
            
            elif config.model_type == ModelType.REGRESSION:
                if algorithm == "random_forest":
                    model = RandomForestClassifier(**params)  # Note: should be RandomForestRegressor
                elif algorithm == "linear_regression":
                    model = LinearRegression(**params)
                elif algorithm == "gradient_boosting":
                    model = GradientBoostingRegressor(**params)
                elif algorithm == "svr":
                    model = SVR(**params)
                elif algorithm == "mlp":
                    model = MLPRegressor(**params)
                else:
                    raise ValueError(f"Unknown regression algorithm: {algorithm}")
            
            else:
                raise ValueError(f"Model type {config.model_type.value} not implemented")
            
            self.logger.info(f"Created {algorithm} model for {config.model_type.value}")
            
            return model
            
        except Exception as e:
            self.logger.error(f"Failed to create model: {e}")
            raise
    
    def train_model(self, model: Any, X_train: pd.DataFrame, y_train: pd.Series,
                   X_val: pd.DataFrame = None, y_val: pd.Series = None) -> Dict[str, float]:
        """Train ML model and return metrics."""
        try:
            start_time = time.time()
            
            # Train model
            model.fit(X_train, y_train)
            
            training_time = time.time() - start_time
            
            # Calculate metrics
            metrics = {"training_time_seconds": training_time}
            
            # Training metrics
            y_train_pred = model.predict(X_train)
            
            if hasattr(model, "predict_proba"):
                # Classification metrics
                metrics["train_accuracy"] = accuracy_score(y_train, y_train_pred)
                metrics["train_precision"] = precision_score(y_train, y_train_pred, average='weighted', zero_division=0)
                metrics["train_recall"] = recall_score(y_train, y_train_pred, average='weighted', zero_division=0)
                metrics["train_f1"] = f1_score(y_train, y_train_pred, average='weighted', zero_division=0)
            else:
                # Regression metrics
                metrics["train_mse"] = mean_squared_error(y_train, y_train_pred)
                metrics["train_r2"] = r2_score(y_train, y_train_pred)
                metrics["train_rmse"] = np.sqrt(metrics["train_mse"])
            
            # Validation metrics
            if X_val is not None and y_val is not None:
                y_val_pred = model.predict(X_val)
                
                if hasattr(model, "predict_proba"):
                    metrics["val_accuracy"] = accuracy_score(y_val, y_val_pred)
                    metrics["val_precision"] = precision_score(y_val, y_val_pred, average='weighted', zero_division=0)
                    metrics["val_recall"] = recall_score(y_val, y_val_pred, average='weighted', zero_division=0)
                    metrics["val_f1"] = f1_score(y_val, y_val_pred, average='weighted', zero_division=0)
                else:
                    metrics["val_mse"] = mean_squared_error(y_val, y_val_pred)
                    metrics["val_r2"] = r2_score(y_val, y_val_pred)
                    metrics["val_rmse"] = np.sqrt(metrics["val_mse"])
            
            self.logger.info(f"Model training completed in {training_time:.2f} seconds")
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Model training failed: {e}")
            raise
    
    def cross_validate_model(self, model: Any, X: pd.DataFrame, y: pd.Series, cv: int = 5) -> Dict[str, float]:
        """Perform cross-validation on model."""
        try:
            if hasattr(model, "predict_proba"):
                # Classification
                scoring = ['accuracy', 'precision_weighted', 'recall_weighted', 'f1_weighted']
            else:
                # Regression
                scoring = ['neg_mean_squared_error', 'r2']
            
            cv_results = {}
            
            for score in scoring:
                scores = cross_val_score(model, X, y, cv=cv, scoring=score)
                cv_results[f"cv_{score}"] = scores.mean()
                cv_results[f"cv_{score}_std"] = scores.std()
            
            self.logger.info(f"Cross-validation completed with {cv} folds")
            
            return cv_results
            
        except Exception as e:
            self.logger.error(f"Cross-validation failed: {e}")
            return {}
    
    def hyperparameter_tuning(self, model: Any, X: pd.DataFrame, y: pd.Series,
                            param_grid: Dict[str, List], cv: int = 5) -> Tuple[Any, Dict[str, Any]]:
        """Perform hyperparameter tuning using grid search."""
        try:
            if hasattr(model, "predict_proba"):
                scoring = 'accuracy'
            else:
                scoring = 'neg_mean_squared_error'
            
            grid_search = GridSearchCV(
                model, param_grid, cv=cv, scoring=scoring, n_jobs=-1, verbose=1
            )
            
            grid_search.fit(X, y)
            
            best_model = grid_search.best_estimator_
            best_params = grid_search.best_params_
            best_score = grid_search.best_score_
            
            tuning_results = {
                "best_params": best_params,
                "best_score": best_score,
                "cv_results": grid_search.cv_results_
            }
            
            self.logger.info(f"Hyperparameter tuning completed. Best score: {best_score:.4f}")
            
            return best_model, tuning_results
            
        except Exception as e:
            self.logger.error(f"Hyperparameter tuning failed: {e}")
            return model, {}
    
    def get_feature_importance(self, model: Any, feature_names: List[str]) -> Dict[str, float]:
        """Get feature importance from trained model."""
        try:
            importance_dict = {}
            
            if hasattr(model, 'feature_importances_'):
                # Tree-based models
                importances = model.feature_importances_
                importance_dict = dict(zip(feature_names, importances))
            
            elif hasattr(model, 'coef_'):
                # Linear models
                if len(model.coef_.shape) == 1:
                    # Binary classification or regression
                    importances = np.abs(model.coef_)
                else:
                    # Multi-class classification
                    importances = np.abs(model.coef_).mean(axis=0)
                
                importance_dict = dict(zip(feature_names, importances))
            
            # Sort by importance
            importance_dict = dict(sorted(importance_dict.items(), key=lambda x: x[1], reverse=True))
            
            return importance_dict
            
        except Exception as e:
            self.logger.error(f"Failed to get feature importance: {e}")
            return {}

class ModelManager:
    """Model lifecycle management."""
    
    def __init__(self, model_registry_path: str = "models"):
        self.model_registry_path = Path(model_registry_path)
        self.model_registry_path.mkdir(exist_ok=True)
        self.logger = logging.getLogger('ModelManager')
        self.models = {}
        self.deployments = {}
        
    def save_model(self, model: Any, model_id: str, metadata: Dict[str, Any] = None) -> str:
        """Save trained model to registry."""
        try:
            model_dir = self.model_registry_path / model_id
            model_dir.mkdir(exist_ok=True)
            
            # Save model
            model_path = model_dir / "model.pkl"
            joblib.dump(model, model_path)
            
            # Save metadata
            if metadata:
                metadata_path = model_dir / "metadata.json"
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2, default=str)
            
            # Calculate model size
            model_size_mb = model_path.stat().st_size / (1024 * 1024)
            
            self.logger.info(f"Saved model {model_id} ({model_size_mb:.2f} MB)")
            
            return str(model_path)
            
        except Exception as e:
            self.logger.error(f"Failed to save model {model_id}: {e}")
            raise
    
    def load_model(self, model_id: str) -> Tuple[Any, Dict[str, Any]]:
        """Load model from registry."""
        try:
            model_dir = self.model_registry_path / model_id
            
            if not model_dir.exists():
                raise FileNotFoundError(f"Model {model_id} not found")
            
            # Load model
            model_path = model_dir / "model.pkl"
            model = joblib.load(model_path)
            
            # Load metadata
            metadata = {}
            metadata_path = model_dir / "metadata.json"
            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
            
            self.logger.info(f"Loaded model {model_id}")
            
            return model, metadata
            
        except Exception as e:
            self.logger.error(f"Failed to load model {model_id}: {e}")
            raise
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all models in registry."""
        models = []
        
        for model_dir in self.model_registry_path.iterdir():
            if model_dir.is_dir():
                model_info = {
                    "model_id": model_dir.name,
                    "created_at": datetime.fromtimestamp(model_dir.stat().st_ctime),
                    "size_mb": 0.0
                }
                
                # Get model size
                model_path = model_dir / "model.pkl"
                if model_path.exists():
                    model_info["size_mb"] = model_path.stat().st_size / (1024 * 1024)
                
                # Get metadata
                metadata_path = model_dir / "metadata.json"
                if metadata_path.exists():
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                        model_info.update(metadata)
                
                models.append(model_info)
        
        return sorted(models, key=lambda x: x["created_at"], reverse=True)
    
    def delete_model(self, model_id: str) -> bool:
        """Delete model from registry."""
        try:
            model_dir = self.model_registry_path / model_id
            
            if model_dir.exists():
                import shutil
                shutil.rmtree(model_dir)
                self.logger.info(f"Deleted model {model_id}")
                return True
            else:
                self.logger.warning(f"Model {model_id} not found")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to delete model {model_id}: {e}")
            return False

class MLPipeline:
    """Complete machine learning pipeline."""
    
    def __init__(self, registry_path: str = "models"):
        self.data_processor = DataProcessor()
        self.model_trainer = ModelTrainer()
        self.model_manager = ModelManager(registry_path)
        self.logger = self._setup_logging()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.training_jobs = {}
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('MLPipeline')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def create_training_job(self, config: ModelConfig, dataset_path: str,
                          target_column: str, data_type: DataType = DataType.TABULAR) -> str:
        """Create and submit training job."""
        job_id = str(uuid.uuid4())
        
        # Load and analyze dataset
        df, dataset_info = self.data_processor.load_dataset(dataset_path, data_type)
        dataset_info.target_column = target_column
        
        training_job = TrainingJob(
            job_id=job_id,
            model_config=config,
            dataset_id=dataset_info.id,
            status=ModelStatus.TRAINING
        )
        
        self.training_jobs[job_id] = training_job
        
        # Submit training job
        future = self.executor.submit(self._execute_training_job, training_job, df, dataset_info)
        
        self.logger.info(f"Created training job: {job_id}")
        
        return job_id
    
    def _execute_training_job(self, job: TrainingJob, df: pd.DataFrame, dataset_info: DatasetInfo):
        """Execute training job."""
        try:
            job.started_at = datetime.now()
            job.status = ModelStatus.TRAINING
            
            # Preprocess data
            processed_df, preprocessing_info = self.data_processor.preprocess_data(
                df, dataset_info, dataset_info.target_column
            )
            
            # Prepare features and target
            X = processed_df.drop(columns=[dataset_info.target_column])
            y = processed_df[dataset_info.target_column]
            
            # Feature selection if specified
            if job.model_config.feature_selection:
                selected_features = self.data_processor.feature_selection(
                    X, y, method=job.model_config.feature_selection
                )
                X = X[selected_features]
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=job.model_config.test_size, 
                random_state=job.model_config.random_state
            )
            
            # Create and train model
            model = self.model_trainer.create_model(job.model_config)
            
            # Train model
            training_metrics = self.model_trainer.train_model(model, X_train, y_train, X_test, y_test)
            
            # Cross-validation
            cv_metrics = self.model_trainer.cross_validate_model(
                model, X_train, y_train, cv=job.model_config.cross_validation_folds
            )
            
            # Feature importance
            feature_importance = self.model_trainer.get_feature_importance(model, X.columns.tolist())
            
            # Combine all metrics
            job.metrics = {**training_metrics, **cv_metrics}
            
            # Save model
            model_metadata = {
                "model_config": asdict(job.model_config),
                "dataset_info": asdict(dataset_info),
                "preprocessing_info": preprocessing_info,
                "metrics": job.metrics,
                "feature_importance": feature_importance,
                "feature_columns": X.columns.tolist()
            }
            
            model_path = self.model_manager.save_model(
                model, job.model_config.model_id, model_metadata
            )
            
            job.model_path = model_path
            job.status = ModelStatus.TRAINED
            job.completed_at = datetime.now()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            self.logger.info(f"Training job {job.job_id} completed successfully")
            
        except Exception as e:
            job.status = ModelStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.now()
            
            if job.started_at:
                job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            self.logger.error(f"Training job {job.job_id} failed: {e}")
    
    def get_job_status(self, job_id: str) -> Optional[TrainingJob]:
        """Get training job status."""
        return self.training_jobs.get(job_id)
    
    def predict(self, model_id: str, input_data: Union[Dict, pd.DataFrame]) -> Any:
        """Make predictions using trained model."""
        try:
            # Load model and metadata
            model, metadata = self.model_manager.load_model(model_id)
            
            # Prepare input data
            if isinstance(input_data, dict):
                input_df = pd.DataFrame([input_data])
            else:
                input_df = input_data.copy()
            
            # Apply same preprocessing as training
            # (This is simplified - would need to store and apply exact preprocessing pipeline)
            
            # Make prediction
            start_time = time.time()
            predictions = model.predict(input_df)
            inference_time = (time.time() - start_time) * 1000  # milliseconds
            
            self.logger.info(f"Prediction completed in {inference_time:.2f}ms")
            
            return predictions.tolist() if hasattr(predictions, 'tolist') else predictions
            
        except Exception as e:
            self.logger.error(f"Prediction failed for model {model_id}: {e}")
            raise
    
    def get_model_metrics(self, model_id: str) -> Dict[str, Any]:
        """Get model performance metrics."""
        try:
            _, metadata = self.model_manager.load_model(model_id)
            return metadata.get("metrics", {})
        except Exception as e:
            self.logger.error(f"Failed to get metrics for model {model_id}: {e}")
            return {}
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all trained models."""
        return self.model_manager.list_models()
    
    def delete_model(self, model_id: str) -> bool:
        """Delete trained model."""
        return self.model_manager.delete_model(model_id)


def main():
    """Example usage of ML Pipeline."""
    # Initialize pipeline
    pipeline = MLPipeline()
    
    try:
        print("🤖 Machine Learning Pipeline")
        
        # Create sample dataset
        print("📊 Creating sample dataset...")
        
        # Generate synthetic data
        np.random.seed(42)
        n_samples = 1000
        
        data = {
            'feature_1': np.random.normal(0, 1, n_samples),
            'feature_2': np.random.normal(2, 1.5, n_samples),
            'feature_3': np.random.uniform(0, 10, n_samples),
            'feature_4': np.random.choice(['A', 'B', 'C'], n_samples),
            'feature_5': np.random.exponential(2, n_samples)
        }
        
        # Create target variable
        data['target'] = (
            data['feature_1'] * 0.5 + 
            data['feature_2'] * 0.3 + 
            data['feature_3'] * 0.1 + 
            np.random.normal(0, 0.1, n_samples)
        )
        
        df = pd.DataFrame(data)
        
        # Save dataset
        dataset_path = "/tmp/sample_dataset.csv"
        df.to_csv(dataset_path, index=False)
        
        print(f"✅ Created dataset with {len(df)} samples")
        
        # Configure model
        model_config = ModelConfig(
            model_id="sample_regression_model",
            name="Sample Regression Model",
            model_type=ModelType.REGRESSION,
            algorithm="random_forest",
            hyperparameters={
                "n_estimators": 100,
                "max_depth": 10,
                "random_state": 42
            },
            feature_selection="correlation",
            cross_validation_folds=5
        )
        
        # Create training job
        print("🚀 Starting model training...")
        job_id = pipeline.create_training_job(
            config=model_config,
            dataset_path=dataset_path,
            target_column="target",
            data_type=DataType.TABULAR
        )
        
        # Monitor training
        print(f"📋 Training job ID: {job_id}")
        
        while True:
            job = pipeline.get_job_status(job_id)
            
            if job.status == ModelStatus.TRAINED:
                print("✅ Training completed successfully!")
                break
            elif job.status == ModelStatus.FAILED:
                print(f"❌ Training failed: {job.error_message}")
                break
            
            print(f"⏳ Status: {job.status.value}")
            time.sleep(2)
        
        # Get model metrics
        if job.status == ModelStatus.TRAINED:
            metrics = pipeline.get_model_metrics(model_config.model_id)
            
            print("\n📈 Model Performance:")
            for metric, value in metrics.items():
                if isinstance(value, float):
                    print(f"   {metric}: {value:.4f}")
                else:
                    print(f"   {metric}: {value}")
            
            # Make sample prediction
            print("\n🔮 Making sample prediction...")
            
            sample_input = {
                'feature_1': 1.0,
                'feature_2': 2.5,
                'feature_3': 5.0,
                'feature_4': 'A',
                'feature_5': 3.0
            }
            
            prediction = pipeline.predict(model_config.model_id, sample_input)
            print(f"   Prediction: {prediction[0]:.4f}")
        
        # List all models
        print("\n📚 Available Models:")
        models = pipeline.list_models()
        
        for model in models:
            print(f"   🤖 {model['model_id']}")
            print(f"      Created: {model['created_at']}")
            print(f"      Size: {model['size_mb']:.2f} MB")
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()