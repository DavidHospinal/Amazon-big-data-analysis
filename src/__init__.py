"""
Amazon Big Data Analysis Project
INF3590 - Big Data
Pontificia Universidad Católica de Chile

Proyecto de análisis de reviews de Amazon implementando un flujo completo de Big Data:
- Adquisición de datos desde Stanford SNAP
- Preprocesamiento y limpieza
- Almacenamiento NoSQL con TinyDB
- Análisis exploratorio y visualizaciones

Autor: Oscar David Hospinal R.
Fecha: Junio 2025
"""

__version__ = "1.0.0"
__author__ = "Oscar David Hospinal R."
__email__ = "oscar.hospinal@example.com"
__course__ = "INF3590 - Big Data"
__university__ = "Pontificia Universidad Católica de Chile"

# Importaciones principales (solo si se importa como módulo)
try:
    from .acquisition import downloader, extractor
    from .preprocessing import cleaner, transformer
    from .storage import nosql_manager, queries
    from .analysis import explorer, visualizer
except ImportError:
    # Si se ejecuta directamente, mostrar información del proyecto
    print("=" * 60)
    print("🎓 AMAZON BIG DATA ANALYSIS PROJECT")
    print("=" * 60)
    print(f"📚 Curso: {__course__}")
    print(f"🏛️  Universidad: {__university__}")
    print(f"👨‍💻 Autor: {__author__}")
    print(f"📅 Versión: {__version__}")
    print()
    print("📋 ESTRUCTURA DEL PROYECTO:")
    print("   📁 src/acquisition/     - Descarga de datos desde Stanford SNAP")
    print("   📁 src/preprocessing/   - Limpieza y transformación de datos")
    print("   📁 src/storage/         - Gestión de base NoSQL (TinyDB)")
    print("   📁 src/analysis/        - Análisis exploratorio y visualizaciones")
    print("   📁 notebooks/           - Jupyter notebooks del flujo completo")
    print("   📁 data/                - Datos procesados y base de datos")
    print()
    print("🚀 PARA EJECUTAR EL PROYECTO:")
    print("   1. Ejecutar notebooks en orden: 01 → 02 → 03 → 04")
    print("   2. O usar los módulos desde Python:")
    print("      import sys")
    print("      sys.path.append('ruta/al/proyecto')")
    print("      from src.acquisition import downloader")
    print()
    print("=" * 60)

if __name__ == "__main__":
    # Este código se ejecuta solo si el archivo se ejecuta directamente
    print("ℹ️  Este es un módulo de inicialización.")
    print("💡 Para usar el proyecto, ejecuta los notebooks o importa los módulos.")