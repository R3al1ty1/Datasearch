"""Utility functions for Kaggle parser module."""
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

from lib.core.container import container
from .models import MetaKaggleConsts


def get_csv_path(cache_dir: Path) -> Path:
    """Returns path to Datasets.csv."""
    return cache_dir / MetaKaggleConsts.CSV_FILENAME


def initialize_kaggle_api() -> KaggleApi:
    """Initializes and authenticates Kaggle API."""
    api = KaggleApi()
    api.authenticate()
    container.logger.info("Kaggle API authenticated")
    return api
