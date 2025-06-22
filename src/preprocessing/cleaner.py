"""
Módulo de limpieza de datos
Funciones para limpiar y validar reviews de Amazon
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
import logging


class DataCleaner:
    """Clase para limpieza y validación de datos de reviews"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def clean_review_data(self, review_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Limpia y valida un registro de review individual

        Args:
            review_data: Diccionario con datos del review

        Returns:
            Diccionario con datos limpios
        """
        cleaned_review = {}

        # Campos obligatorios con valores por defecto (CORREGIDO)
        reviewerID = review_data.get('reviewerID', 'UNKNOWN')
        cleaned_review['reviewerID'] = reviewerID if reviewerID and reviewerID.strip() else 'UNKNOWN'

        asin = review_data.get('asin', 'UNKNOWN')
        cleaned_review['asin'] = asin if asin else 'UNKNOWN'

        reviewerName = review_data.get('reviewerName', 'Anonymous')
        cleaned_review['reviewerName'] = reviewerName if reviewerName and reviewerName.strip() else 'Anonymous'

        # Limpieza de campo helpful
        helpful = review_data.get('helpful', [0, 0])
        if isinstance(helpful, list) and len(helpful) >= 2:
            cleaned_review['helpful'] = helpful[:2]
        else:
            cleaned_review['helpful'] = [0, 0]

        # Limpieza de texto de review
        review_text = review_data.get('reviewText', '')
        if isinstance(review_text, str):
            cleaned_review['reviewText'] = review_text.strip()[:1000]
        else:
            cleaned_review['reviewText'] = ''

        # Validación y normalización de rating
        overall = review_data.get('overall', 3.0)
        try:
            overall_float = float(overall)
            if 1.0 <= overall_float <= 5.0:
                cleaned_review['overall'] = overall_float
            else:
                cleaned_review['overall'] = 3.0
        except (ValueError, TypeError):
            cleaned_review['overall'] = 3.0

        # Limpieza de summary
        summary = review_data.get('summary', '')
        if isinstance(summary, str):
            cleaned_review['summary'] = summary.strip()[:200]
        else:
            cleaned_review['summary'] = ''

        # Validación de timestamp
        unix_time = review_data.get('unixReviewTime', 0)
        try:
            cleaned_review['unixReviewTime'] = int(unix_time)
        except (ValueError, TypeError):
            cleaned_review['unixReviewTime'] = 0

        # Limpieza de fecha legible
        review_time = review_data.get('reviewTime', '')
        if isinstance(review_time, str):
            cleaned_review['reviewTime'] = review_time.strip()
        else:
            cleaned_review['reviewTime'] = ''

        return cleaned_review

    def validate_review_quality(self, review_data: Dict[str, Any]) -> bool:
        """
        Valida la calidad del registro de review

        Args:
            review_data: Diccionario con datos del review

        Returns:
            True si el registro es válido
        """
        # Verificar campos obligatorios
        required_fields = ['reviewerID', 'asin', 'overall']
        for field in required_fields:
            if field not in review_data or not review_data[field]:
                return False

        # Verificar que overall esté en rango válido
        if not (1.0 <= review_data['overall'] <= 5.0):
            return False

        # Verificar que tenga contenido mínimo
        has_content = (review_data.get('reviewText', '').strip() or
                       review_data.get('summary', '').strip())

        return bool(has_content)

    def clean_batch(self, reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Limpia un lote de reviews

        Args:
            reviews: Lista de diccionarios con reviews

        Returns:
            Lista de reviews limpios y válidos
        """
        cleaned_reviews = []

        for review in reviews:
            cleaned_review = self.clean_review_data(review)
            if self.validate_review_quality(cleaned_review):
                cleaned_reviews.append(cleaned_review)

        self.logger.info(f"Limpiados {len(cleaned_reviews)} de {len(reviews)} reviews")
        return cleaned_reviews


if __name__ == "__main__":
    print("🧹 MÓDULO DE LIMPIEZA DE DATOS")
    print("=" * 40)
    print("📝 DataCleaner - Clase principal de limpieza")
    print("🔧 Funciones disponibles:")
    print("   • clean_review_data()")
    print("   • validate_review_quality()")
    print("   • clean_batch()")
    print()
    print("💡 Para usar:")
    print("   from src.preprocessing.cleaner import DataCleaner")
    print("   cleaner = DataCleaner()")
    print("   cleaned = cleaner.clean_review_data(review)")