"""
Módulo de análisis exploratorio
Exploración de datos y visualizaciones
"""

try:
    from .explorer import DataExplorer
    from .visualizer import DataVisualizer
    __all__ = ['DataExplorer', 'DataVisualizer']
except ImportError:
    print("📋 MÓDULO DE ANÁLISIS EXPLORATORIO")
    print("=" * 40)
    print("🔍 explorer.py    - Análisis estadístico")
    print("📊 visualizer.py  - Visualizaciones")
    print()
    print("💡 Para usar este módulo:")
    print("   from src.analysis import DataExplorer, DataVisualizer")

if __name__ == "__main__":
    print("ℹ️  Este es un archivo de configuración de módulo.")