"""
Módulo de preprocesamiento de datos
Limpieza, transformación y enriquecimiento de reviews de Amazon
"""

try:
    # Importaciones normales cuando se usa como módulo
    from .cleaner import DataCleaner
    from .transformer import DataTransformer
    __all__ = ['DataCleaner', 'DataTransformer']
except ImportError:
    # Si se ejecuta directamente, mostrar información
    print("📋 MÓDULO DE PREPROCESAMIENTO")
    print("=" * 40)
    print("🧹 cleaner.py      - Limpieza y validación de datos")
    print("🔄 transformer.py  - Transformación y enriquecimiento")
    print()
    print("💡 Para usar este módulo:")
    print("   from src.preprocessing import DataCleaner, DataTransformer")
    print("   # O ejecuta los notebooks en orden")

if __name__ == "__main__":
    print("ℹ️  Este es un archivo de configuración de módulo.")
    print("🚀 Ejecuta los notebooks para el análisis completo.")