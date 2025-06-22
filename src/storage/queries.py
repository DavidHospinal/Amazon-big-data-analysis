"""
Motor de consultas para base de datos NoSQL
Consultas predefinidas y análisis de datos
"""

from tinydb import Query
from typing import List, Dict, Any, Optional
import pandas as pd
import logging


class QueryEngine:
    """Motor de consultas para análisis de reviews"""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        self.Review = Query()

    def get_high_rating_reviews(self, min_rating: float = 4.5) -> List[Dict[str, Any]]:
        """
        Obtiene reviews con rating alto

        Args:
            min_rating: Rating mínimo

        Returns:
            Lista de reviews con rating alto
        """
        return self.db_manager.reviews_table.search(self.Review.overall >= min_rating)

    def get_reviews_by_category(self, category: str) -> List[Dict[str, Any]]:
        """
        Obtiene reviews por categoría

        Args:
            category: Nombre de la categoría

        Returns:
            Lista de reviews de la categoría
        """
        return self.db_manager.reviews_table.search(self.Review.original_category == category)

    def get_category_statistics(self) -> Dict[str, Dict[str, float]]:
        """
        Calcula estadísticas por categoría

        Returns:
            Diccionario con estadísticas por categoría
        """
        all_reviews = self.db_manager.reviews_table.all()
        df = pd.DataFrame(all_reviews)

        if 'original_category' not in df.columns:
            return {}

        stats = df.groupby('original_category')['overall'].agg([
            'count', 'mean', 'std', 'min', 'max'
        ]).round(3)

        return stats.to_dict('index')


if __name__ == "__main__":
    print("🔍 MÓDULO DE CONSULTAS NoSQL")
    print("=" * 40)
    print("📝 QueryEngine - Motor de consultas principal")
    print("🔧 Funciones disponibles:")
    print("   • get_high_rating_reviews()")
    print("   • get_reviews_by_category()")
    print("   • get_category_statistics()")
    print()
    print("💡 Para usar:")
    print("   from src.storage.queries import QueryEngine")
    print("   engine = QueryEngine(db_manager)")
    print("   high_reviews = engine.get_high_rating_reviews(4.5)")