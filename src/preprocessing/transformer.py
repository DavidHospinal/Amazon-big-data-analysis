"""
Módulo de transformación de datos
Enriquecimiento y creación de nuevas variables
"""

from datetime import datetime
from typing import Dict, Any
import logging


class DataTransformer:
    """Clase para transformación y enriquecimiento de datos"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.category_mapping = {
            'Books': 'Entertainment',
            'Video_Games': 'Entertainment',
            'Movies_and_TV': 'Entertainment',
            'Home_and_Kitchen': 'Home',
            'Tools_and_Home_Improvement': 'Home',
            'Patio_Lawn_and_Garden': 'Home'
        }

    def enrich_review_data(self, review_data: Dict[str, Any], category_name: str) -> Dict[str, Any]:
        """
        Enriquece el registro con campos adicionales
        """
        # Agregar grupo de categoría
        review_data['category_group'] = self.category_mapping.get(category_name, 'Other')

        # Agregar tipo de análisis basado en grupo
        if review_data['category_group'] == 'Entertainment':
            review_data['analysis_type'] = 'Leisure/Personal'
        elif review_data['category_group'] == 'Home':
            review_data['analysis_type'] = 'Practical/Utility'
        else:
            review_data['analysis_type'] = 'General'

        # Agregar timestamp de procesamiento
        review_data['download_timestamp'] = datetime.now().timestamp()

        # Agregar categoría original
        review_data['original_category'] = category_name

        return review_data


if __name__ == "__main__":
    print("🔄 MÓDULO DE TRANSFORMACIÓN DE DATOS")
    print("=" * 40)
    print("📝 DataTransformer - Clase principal de transformación")
    print("🔧 Funciones disponibles:")
    print("   • enrich_review_data()")
    print()
    print("💡 Para usar:")
    print("   from src.preprocessing.transformer import DataTransformer")
    print("   transformer = DataTransformer()")
    print("   enriched = transformer.enrich_review_data(review, 'Books')")