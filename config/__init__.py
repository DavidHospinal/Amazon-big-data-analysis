"""
Módulo de configuración para Amazon Big Data Analysis
Configuraciones centralizadas del proyecto
"""

from .settings import (
    PROJECT_CONFIG,
    DATA_CONFIG,
    ANALYSIS_CONFIG,
    get_data_path,
    get_output_path
)

from .database import (
    DATABASE_CONFIG,
    get_db_connection,
    get_table_config
)

__version__ = "1.0.0"
__config_version__ = "1.0.0"

# Configuraciones principales exportadas
__all__ = [
    'PROJECT_CONFIG',
    'DATA_CONFIG',
    'ANALYSIS_CONFIG',
    'DATABASE_CONFIG',
    'get_data_path',
    'get_output_path',
    'get_db_connection',
    'get_table_config'
]


def validate_config():
    """
    Valida que todas las configuraciones estén correctamente definidas

    Returns:
        bool: True si todas las configuraciones son válidas
    """
    try:
        # Verificar configuraciones principales
        assert PROJECT_CONFIG is not None
        assert DATA_CONFIG is not None
        assert DATABASE_CONFIG is not None

        # Verificar rutas críticas
        data_path = get_data_path()
        assert data_path.exists(), f"Directorio de datos no existe: {data_path}"

        return True

    except Exception as e:
        print(f"❌ Error en validación de configuración: {e}")
        return False


if __name__ == "__main__":
    print("⚙️ MÓDULO DE CONFIGURACIÓN")
    print("=" * 40)
    print("📋 Configuraciones disponibles:")
    print("   • PROJECT_CONFIG - Configuración general del proyecto")
    print("   • DATA_CONFIG - Configuración de datos y rutas")
    print("   • ANALYSIS_CONFIG - Parámetros de análisis")
    print("   • DATABASE_CONFIG - Configuración de base NoSQL")
    print()
    print("🔧 Funciones de utilidad:")
    print("   • get_data_path() - Obtener ruta de datos")
    print("   • get_output_path() - Obtener ruta de salida")
    print("   • get_db_connection() - Configuración de BD")
    print("   • validate_config() - Validar configuraciones")
    print()

    # Validar configuración al ejecutar
    if validate_config():
        print("✅ Todas las configuraciones son válidas")
    else:
        print("❌ Problemas detectados en configuración")