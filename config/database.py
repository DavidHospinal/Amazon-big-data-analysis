"""
Configuración de base de datos NoSQL
Parámetros y configuraciones para TinyDB
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from tinydb import TinyDB

# Importar configuraciones base
from .settings import DATA_DIR, BASE_DIR

# ==================== CONFIGURACIÓN DE BASE DE DATOS ====================

DATABASE_CONFIG = {
    'engine': 'TinyDB',
    'version': '4.8.0',
    'database_file': DATA_DIR / "amazon_reviews.json",
    'backup_dir': DATA_DIR / "backups",
    'encoding': 'utf-8',
    'indent': 2,
    'ensure_ascii': False,

    # Configuración de tablas
    'tables': {
        'reviews': {
            'description': 'Tabla principal con todos los reviews',
            'primary_key': None,  # TinyDB maneja automáticamente
            'indexes': ['overall', 'original_category', 'category_group'],
            'estimated_size': 1200
        },
        'books': {
            'description': 'Reviews de libros',
            'filter_field': 'original_category',
            'filter_value': 'Books',
            'estimated_size': 200
        },
        'video_games': {
            'description': 'Reviews de videojuegos',
            'filter_field': 'original_category',
            'filter_value': 'Video_Games',
            'estimated_size': 200
        },
        'movies_tv': {
            'description': 'Reviews de películas y TV',
            'filter_field': 'original_category',
            'filter_value': 'Movies_and_TV',
            'estimated_size': 200
        },
        'home_kitchen': {
            'description': 'Reviews de hogar y cocina',
            'filter_field': 'original_category',
            'filter_value': 'Home_and_Kitchen',
            'estimated_size': 200
        },
        'tools': {
            'description': 'Reviews de herramientas',
            'filter_field': 'original_category',
            'filter_value': 'Tools_and_Home_Improvement',
            'estimated_size': 200
        },
        'patio_garden': {
            'description': 'Reviews de patio y jardín',
            'filter_field': 'original_category',
            'filter_value': 'Patio_Lawn_and_Garden',
            'estimated_size': 200
        },
        'metadata': {
            'description': 'Metadata del sistema y estadísticas',
            'estimated_size': 1
        }
    },

    # Configuración de queries
    'query_config': {
        'default_limit': 1000,
        'max_limit': 10000,
        'timeout_seconds': 30,
        'cache_results': True,
        'cache_ttl_minutes': 15
    },

    # Configuración de backup
    'backup_config': {
        'auto_backup': True,
        'backup_frequency': 'daily',
        'max_backups': 7,
        'compress_backups': True
    }
}

# ==================== ESQUEMAS DE DATOS ====================

REVIEW_SCHEMA = {
    'reviewerID': {
        'type': 'string',
        'required': True,
        'description': 'ID único del reviewer'
    },
    'asin': {
        'type': 'string',
        'required': True,
        'description': 'ID único del producto Amazon'
    },
    'reviewerName': {
        'type': 'string',
        'required': False,
        'default': 'Anonymous',
        'description': 'Nombre del reviewer'
    },
    'helpful': {
        'type': 'array',
        'items': 'integer',
        'length': 2,
        'description': '[votos útiles, total votos]'
    },
    'reviewText': {
        'type': 'string',
        'max_length': 1000,
        'description': 'Texto del review'
    },
    'overall': {
        'type': 'float',
        'minimum': 1.0,
        'maximum': 5.0,
        'required': True,
        'description': 'Rating de 1 a 5 estrellas'
    },
    'summary': {
        'type': 'string',
        'max_length': 200,
        'description': 'Resumen del review'
    },
    'unixReviewTime': {
        'type': 'integer',
        'description': 'Timestamp Unix del review'
    },
    'reviewTime': {
        'type': 'string',
        'description': 'Fecha legible del review'
    },
    # Campos enriquecidos
    'category_group': {
        'type': 'string',
        'enum': ['Entertainment', 'Home'],
        'description': 'Grupo de categoría (Entertainment/Home)'
    },
    'analysis_type': {
        'type': 'string',
        'enum': ['Leisure/Personal', 'Practical/Utility'],
        'description': 'Tipo de análisis'
    },
    'original_category': {
        'type': 'string',
        'required': True,
        'description': 'Categoría original del producto'
    },
    'download_timestamp': {
        'type': 'float',
        'description': 'Timestamp de procesamiento'
    }
}

METADATA_SCHEMA = {
    'database_created': {
        'type': 'string',
        'description': 'Fecha de creación de la BD'
    },
    'total_records': {
        'type': 'integer',
        'description': 'Total de registros'
    },
    'categories_count': {
        'type': 'integer',
        'description': 'Número de categorías'
    },
    'processing_time_seconds': {
        'type': 'float',
        'description': 'Tiempo de procesamiento'
    },
    'data_source': {
        'type': 'string',
        'description': 'Fuente de los datos'
    }
}

# ==================== FUNCIONES DE UTILIDAD ====================

def get_db_connection(db_path: Optional[Path] = None) -> TinyDB:
    """
    Obtiene conexión a la base de datos TinyDB

    Args:
        db_path: Ruta personalizada de la base de datos

    Returns:
        TinyDB: Instancia de la base de datos
    """
    if db_path is None:
        db_path = DATABASE_CONFIG['database_file']

    # Crear directorio si no existe
    db_path.parent.mkdir(exist_ok=True, parents=True)

    return TinyDB(
        db_path,
        indent=DATABASE_CONFIG['indent'],
        ensure_ascii=DATABASE_CONFIG['ensure_ascii']
    )

def get_table_config(table_name: str) -> Dict[str, Any]:
    """
    Obtiene configuración de una tabla específica

    Args:
        table_name: Nombre de la tabla

    Returns:
        dict: Configuración de la tabla
    """
    return DATABASE_CONFIG['tables'].get(table_name, {})

def get_table_names() -> list:
    """
    Obtiene lista de nombres de tablas configuradas

    Returns:
        list: Nombres de tablas
    """
    return list(DATABASE_CONFIG['tables'].keys())

def validate_review_schema(review: Dict[str, Any]) -> bool:
    """
    Valida que un review cumpla con el esquema definido

    Args:
        review: Diccionario con datos del review

    Returns:
        bool: True si es válido
    """
    try:
        # Verificar campos requeridos
        required_fields = [field for field, config in REVIEW_SCHEMA.items()
                          if config.get('required', False)]

        for field in required_fields:
            if field not in review:
                return False

        # Verificar tipos básicos
        if 'overall' in review:
            rating = review['overall']
            if not isinstance(rating, (int, float)) or not (1.0 <= rating <= 5.0):
                return False

        return True

    except Exception:
        return False

def create_backup(db_path: Optional[Path] = None) -> Path:
    """
    Crea backup de la base de datos

    Args:
        db_path: Ruta de la base de datos

    Returns:
        Path: Ruta del archivo de backup
    """
    import shutil
    from datetime import datetime

    if db_path is None:
        db_path = DATABASE_CONFIG['database_file']

    backup_dir = DATABASE_CONFIG['backup_dir']
    backup_dir.mkdir(exist_ok=True, parents=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f"amazon_reviews_backup_{timestamp}.json"
    backup_path = backup_dir / backup_filename

    if db_path.exists():
        shutil.copy2(db_path, backup_path)
        return backup_path
    else:
        raise FileNotFoundError(f"Base de datos no encontrada: {db_path}")

def get_database_info() -> Dict[str, Any]:
    """
    Obtiene información completa de la configuración de BD

    Returns:
        dict: Información de la base de datos
    """
    db_path = DATABASE_CONFIG['database_file']

    info = {
        'engine': DATABASE_CONFIG['engine'],
        'database_path': str(db_path),
        'database_exists': db_path.exists(),
        'total_tables': len(DATABASE_CONFIG['tables']),
        'table_names': get_table_names(),
        'estimated_total_records': sum(
            table.get('estimated_size', 0)
            for table in DATABASE_CONFIG['tables'].values()
        )
    }

    if db_path.exists():
        info['database_size_mb'] = db_path.stat().st_size / 1024 / 1024

    return info

if __name__ == "__main__":
    print("🗄️ CONFIGURACIÓN DE BASE DE DATOS")
    print("=" * 50)

    # Mostrar información de la BD
    db_info = get_database_info()
    print(f"🔧 Motor: {db_info['engine']}")
    print(f"📁 Archivo: {db_info['database_path']}")
    print(f"✅ Existe: {db_info['database_exists']}")
    print(f"📊 Tablas: {db_info['total_tables']}")
    print(f"📈 Registros estimados: {db_info['estimated_total_records']:,}")

    if 'database_size_mb' in db_info:
        print(f"💾 Tamaño: {db_info['database_size_mb']:.2f} MB")

    print(f"\n📋 Tablas configuradas:")
    for table_name, config in DATABASE_CONFIG['tables'].items():
        print(f"   • {table_name:15} - {config['description']}")