"""
Módulo de almacenamiento NoSQL
Gestión de base de datos TinyDB y consultas
"""

try:
    from .nosql_manager import NoSQLManager
    from .queries import QueryEngine
    __all__ = ['NoSQLManager', 'QueryEngine']
except ImportError:
    print("📋 MÓDULO DE ALMACENAMIENTO NoSQL")
    print("=" * 40)
    print("🗄️  nosql_manager.py - Gestión de TinyDB")
    print("🔍 queries.py       - Motor de consultas")
    print()
    print("💡 Para usar este módulo:")
    print("   from src.storage import NoSQLManager, QueryEngine")

if __name__ == "__main__":
    print("ℹ️  Este es un archivo de configuración de módulo.")