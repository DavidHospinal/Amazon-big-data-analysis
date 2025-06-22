"""
Configuraciones generales del proyecto Amazon Big Data Analysis
Parámetros, rutas y constantes del sistema
"""

import os
from pathlib import Path
from datetime import datetime

# ==================== CONFIGURACIÓN DEL PROYECTO ====================

PROJECT_CONFIG = {
    'name': 'Amazon Big Data Analysis',
    'version': '1.0.0',
    'description': 'Análisis de reviews de Amazon con flujo completo de Big Data',
    'author': 'Oscar David Hospinal R.',
    'course': 'INF3590 - Big Data',
    'university': 'Pontificia Universidad Católica de Chile',
    'due_date': '2025-07-02',
    'created_date': '2025-06-21',
    'encoding': 'utf-8'
}

# ==================== CONFIGURACIÓN DE DATOS ====================

# Rutas del proyecto
BASE_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = BASE_DIR / "data"
SRC_DIR = BASE_DIR / "src"
NOTEBOOKS_DIR = BASE_DIR / "notebooks"
TESTS_DIR = BASE_DIR / "tests"
OUTPUT_DIR = BASE_DIR / "output"

# Subdirectorios de datos
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
SAMPLES_DATA_DIR = DATA_DIR / "samples"

DATA_CONFIG = {
    'base_dir': BASE_DIR,
    'data_dir': DATA_DIR,
    'raw_data_dir': RAW_DATA_DIR,
    'processed_data_dir': PROCESSED_DATA_DIR,
    'samples_data_dir': SAMPLES_DATA_DIR,
    'output_dir': OUTPUT_DIR,

    # Archivos principales
    'database_file': DATA_DIR / "amazon_reviews.json",
    'representative_sample': SAMPLES_DATA_DIR / "final_representative_sample.json",
    'preprocessing_summary': SAMPLES_DATA_DIR / "preprocessing_summary.json",

    # Configuración de descarga
    'download_source': 'Stanford SNAP Amazon Reviews',
    'download_base_url': 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/',
    'target_records_per_category': 200,
    'max_file_size_gb': 5.0,

    # Categorías del proyecto
    'categories': {
        'Books': 'Entertainment',
        'Video_Games': 'Entertainment',
        'Movies_and_TV': 'Entertainment',
        'Home_and_Kitchen': 'Home',
        'Tools_and_Home_Improvement': 'Home',
        'Patio_Lawn_and_Garden': 'Home'
    },

    # Archivos de categorías
    'category_files': {
        'Books': 'reviews_Books.json.gz',
        'Video_Games': 'reviews_Video_Games.json.gz',
        'Movies_and_TV': 'reviews_Movies_and_TV.json.gz',
        'Home_and_Kitchen': 'reviews_Home_and_Kitchen.json.gz',
        'Tools_and_Home_Improvement': 'reviews_Tools_and_Home_Improvement.json.gz',
        'Patio_Lawn_and_Garden': 'reviews_Patio_Lawn_and_Garden.json.gz'
    }
}

# ==================== CONFIGURACIÓN DE ANÁLISIS ====================

ANALYSIS_CONFIG = {
    # Parámetros de calidad de datos
    'min_rating': 1.0,
    'max_rating': 5.0,
    'min_text_length': 10,
    'max_text_length': 1000,
    'max_summary_length': 200,

    # Umbrales de satisfacción
    'satisfaction_thresholds': {
        'excellent': 4.5,
        'good': 3.5,
        'average': 2.5,
        'poor': 0.0
    },

    # Configuración de muestreo
    'sample_size_per_category': 50,
    'total_sample_size': 300,
    'random_seed': 42,
    'stratified_sampling': True,

    # Parámetros de visualización
    'figure_size': (12, 8),
    'dpi': 300,
    'color_palette': 'husl',
    'plot_style': 'default',

    # Métricas de análisis
    'min_reviews_per_product': 2,
    'min_reviews_per_reviewer': 1,
    'temporal_analysis_min_records': 10,

    # Configuración de reportes
    'report_format': 'both',  # 'html', 'pdf', 'both'
    'include_interactive_plots': True,
    'include_statistical_tests': True
}

# ==================== CONFIGURACIÓN DE LOGGING ====================

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s'
        }
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'DEBUG',
            'formatter': 'detailed',
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'amazon_analysis.log',
            'mode': 'a',
        }
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

# ==================== FUNCIONES DE UTILIDAD ====================

def get_data_path(filename: str = None) -> Path:
    """
    Obtiene la ruta del directorio de datos o un archivo específico

    Args:
        filename: Nombre del archivo (opcional)

    Returns:
        Path: Ruta del directorio o archivo
    """
    if filename:
        return DATA_DIR / filename
    return DATA_DIR

def get_output_path(filename: str = None) -> Path:
    """
    Obtiene la ruta del directorio de salida o un archivo específico

    Args:
        filename: Nombre del archivo (opcional)

    Returns:
        Path: Ruta del directorio o archivo
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    if filename:
        return OUTPUT_DIR / filename
    return OUTPUT_DIR

def get_category_mapping() -> dict:
    """
    Obtiene el mapeo de categorías a grupos

    Returns:
        dict: Mapeo categoría -> grupo
    """
    return DATA_CONFIG['categories'].copy()

def get_satisfaction_thresholds() -> dict:
    """
    Obtiene los umbrales de satisfacción configurados

    Returns:
        dict: Umbrales de satisfacción
    """
    return ANALYSIS_CONFIG['satisfaction_thresholds'].copy()

def create_directories():
    """
    Crea todos los directorios necesarios del proyecto
    """
    directories = [
        DATA_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        SAMPLES_DATA_DIR,
        OUTPUT_DIR,
        BASE_DIR / 'logs'
    ]

    for directory in directories:
        directory.mkdir(exist_ok=True, parents=True)

    print(f"✅ Directorios creados: {len(directories)} directorios")

def get_project_info() -> dict:
    """
    Obtiene información completa del proyecto

    Returns:
        dict: Información del proyecto
    """
    return {
        **PROJECT_CONFIG,
        'base_directory': str(BASE_DIR),
        'data_directory': str(DATA_DIR),
        'total_categories': len(DATA_CONFIG['categories']),
        'target_records': len(DATA_CONFIG['categories']) * DATA_CONFIG['target_records_per_category']
    }

if __name__ == "__main__":
    print("⚙️ CONFIGURACIÓN DEL PROYECTO")
    print("=" * 50)

    # Mostrar información del proyecto
    info = get_project_info()
    print(f"📚 Proyecto: {info['name']}")
    print(f"👨‍💻 Autor: {info['author']}")
    print(f"🎓 Curso: {info['course']}")
    print(f"📅 Fecha límite: {info['due_date']}")
    print(f"📁 Directorio base: {info['base_directory']}")
    print(f"🎯 Categorías: {info['total_categories']}")
    print(f"📊 Registros objetivo: {info['target_records']:,}")

    # Crear directorios
    create_directories()