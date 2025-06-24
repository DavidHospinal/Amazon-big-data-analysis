"""
Amazon Data Acquisition Module
=============================
Módulo para adquisición y extracción de datos de Amazon Reviews

Componentes:
- downloader: Descarga datos desde Stanford SNAP
- extractor: Procesa y extrae características de los datos

Autor: [Oscar David Hospinal R.]
Proyecto: Amazon Big Data Analysis
Curso: INF3590 - Big Data
"""

from .downloader import AmazonDataDownloader
from .extractor import AmazonDataExtractor

__version__ = "1.0.0"
__author__ = "Oscar David Hospinal R."

__all__ = [
    "AmazonDataDownloader",
    "AmazonDataExtractor"
]

# Configuración por defecto
DEFAULT_CATEGORIES = {
    "Books": "Entertainment",
    "Video_Games": "Entertainment",
    "Movies_and_TV": "Entertainment",
    "Home_and_Kitchen": "Home",
    "Tools_and_Home_Improvement": "Home",
    "Patio_Lawn_and_Garden": "Home"
}


def get_downloader(data_dir="../../data"):
    """
    Factory function para crear un downloader configurado

    Args:
        data_dir: Directorio base para datos

    Returns:
        AmazonDataDownloader configurado
    """
    return AmazonDataDownloader(data_dir)


def get_extractor(data_dir="../../data"):
    """
    Factory function para crear un extractor configurado

    Args:
        data_dir: Directorio base para datos

    Returns:
        AmazonDataExtractor configurado
    """
    return AmazonDataExtractor(data_dir)