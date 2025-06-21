"""
Amazon Reviews Data Extractor
============================
Utilidades para extraer y procesar datos específicos de las reseñas

Autor: [Oscar David Hospinal R.]
Proyecto: Amazon Big Data Analysis
Curso: INF3590 - Big Data
"""

import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class AmazonDataExtractor:
    """
    Extractor de datos específicos para análisis
    """

    def __init__(self, data_dir: str = "../../data"):
        """
        Inicializa el extractor

        Args:
            data_dir: Directorio base de datos
        """
        self.script_dir = Path(__file__).parent
        self.data_dir = (self.script_dir / data_dir).resolve()
        self.processed_dir = self.data_dir / "processed"
        self.samples_dir = self.data_dir / "samples"

    def load_category_data(self, category_name: str) -> Optional[List[Dict]]:
        """
        Carga datos de una categoría específica

        Args:
            category_name: Nombre de la categoría

        Returns:
            Lista de registros o None si no existe
        """
        file_path = self.processed_dir / f"{category_name}_sample.json"

        if not file_path.exists():
            logger.error(f"❌ Archivo no encontrado: {file_path}")
            return None

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            logger.info(f"✅ Cargados {len(data)} registros de {category_name}")
            return data

        except Exception as e:
            logger.error(f"❌ Error cargando {category_name}: {str(e)}")
            return None

    def load_all_data(self) -> Dict[str, List[Dict]]:
        """
        Carga todos los datos disponibles

        Returns:
            Diccionario con todos los datos por categoría
        """
        categories = [
            "Books", "Video_Games", "Movies_and_TV",
            "Home_and_Kitchen", "Tools_and_Home_Improvement", "Patio_Lawn_and_Garden"
        ]

        all_data = {}

        for category in categories:
            data = self.load_category_data(category)
            if data:
                all_data[category] = data

        logger.info(f"📊 Cargadas {len(all_data)} categorías")
        return all_data

    def extract_basic_stats(self, data: List[Dict]) -> Dict:
        """
        Extrae estadísticas básicas de un dataset

        Args:
            data: Lista de registros

        Returns:
            Diccionario con estadísticas básicas
        """
        if not data:
            return {}

        # Convertir a DataFrame para análisis más fácil
        df = pd.DataFrame(data)

        stats = {
            "total_records": len(data),
            "unique_users": df['reviewerID'].nunique() if 'reviewerID' in df.columns else 0,
            "unique_products": df['asin'].nunique() if 'asin' in df.columns else 0,
            "rating_distribution": {},
            "date_range": {},
            "review_text_stats": {}
        }

        # Distribución de ratings
        if 'overall' in df.columns:
            rating_counts = df['overall'].value_counts().sort_index()
            stats["rating_distribution"] = rating_counts.to_dict()
            stats["avg_rating"] = df['overall'].mean()
            stats["rating_std"] = df['overall'].std()

        # Rango de fechas
        if 'reviewTime' in df.columns:
            try:
                # Convertir fechas
                df['parsed_date'] = pd.to_datetime(df['reviewTime'], errors='coerce')
                valid_dates = df['parsed_date'].dropna()

                if not valid_dates.empty:
                    stats["date_range"] = {
                        "earliest": valid_dates.min().strftime('%Y-%m-%d'),
                        "latest": valid_dates.max().strftime('%Y-%m-%d'),
                        "span_years": (valid_dates.max() - valid_dates.min()).days / 365.25
                    }
            except Exception as e:
                logger.warning(f"⚠️ Error procesando fechas: {str(e)}")

        # Estadísticas de texto
        if 'reviewText' in df.columns:
            text_data = df['reviewText'].dropna()
            if not text_data.empty:
                text_lengths = text_data.str.len()
                stats["review_text_stats"] = {
                    "avg_length": text_lengths.mean(),
                    "median_length": text_lengths.median(),
                    "max_length": text_lengths.max(),
                    "min_length": text_lengths.min(),
                    "reviews_with_text": len(text_data)
                }

        return stats

    def extract_category_comparison(self, all_data: Dict[str, List[Dict]]) -> Dict:
        """
        Extrae comparaciones entre categorías

        Args:
            all_data: Datos de todas las categorías

        Returns:
            Diccionario con comparaciones
        """
        comparison = {
            "category_stats": {},
            "entertainment_vs_home": {},
            "cross_category_users": {}
        }

        # Stats por categoría
        for category, data in all_data.items():
            comparison["category_stats"][category] = self.extract_basic_stats(data)

        # Separar Entertainment vs Home
        entertainment_data = []
        home_data = []

        for category, data in all_data.items():
            if any(cat in category for cat in ["Books", "Video_Games", "Movies"]):
                entertainment_data.extend(data)
            else:
                home_data.extend(data)

        if entertainment_data and home_data:
            comparison["entertainment_vs_home"] = {
                "entertainment": self.extract_basic_stats(entertainment_data),
                "home": self.extract_basic_stats(home_data)
            }

        # Usuarios que aparecen en múltiples categorías
        all_users = {}
        for category, data in all_data.items():
            for record in data:
                user_id = record.get('reviewerID')
                if user_id:
                    if user_id not in all_users:
                        all_users[user_id] = []
                    all_users[user_id].append(category)

        # Usuarios cross-category
        cross_category_users = {
            user_id: categories
            for user_id, categories in all_users.items()
            if len(set(categories)) > 1
        }

        comparison["cross_category_users"] = {
            "total_cross_users": len(cross_category_users),
            "percentage": len(cross_category_users) / len(all_users) * 100 if all_users else 0,
            "sample_users": dict(list(cross_category_users.items())[:10])  # Muestra
        }

        return comparison

    def extract_temporal_patterns(self, all_data: Dict[str, List[Dict]]) -> Dict:
        """
        Extrae patrones temporales de las reseñas

        Args:
            all_data: Datos de todas las categorías

        Returns:
            Diccionario con patrones temporales
        """
        temporal_patterns = {
            "monthly_activity": {},
            "yearly_trends": {},
            "seasonal_patterns": {}
        }

        # Combinar todos los datos con categoría
        all_records = []
        for category, data in all_data.items():
            for record in data:
                record_copy = record.copy()
                record_copy['category'] = category
                all_records.append(record_copy)

        if not all_records:
            return temporal_patterns

        # Convertir a DataFrame
        df = pd.DataFrame(all_records)

        if 'reviewTime' in df.columns:
            try:
                # Parsear fechas
                df['parsed_date'] = pd.to_datetime(df['reviewTime'], errors='coerce')
                df_with_dates = df.dropna(subset=['parsed_date'])

                if not df_with_dates.empty:
                    # Extraer componentes temporales
                    df_with_dates['year'] = df_with_dates['parsed_date'].dt.year
                    df_with_dates['month'] = df_with_dates['parsed_date'].dt.month
                    df_with_dates['season'] = df_with_dates['month'].apply(self._get_season)

                    # Actividad mensual
                    monthly_activity = df_with_dates.groupby(['category', 'month']).size().reset_index(name='count')
                    temporal_patterns["monthly_activity"] = monthly_activity.to_dict('records')

                    # Tendencias anuales
                    yearly_trends = df_with_dates.groupby(['category', 'year']).size().reset_index(name='count')
                    temporal_patterns["yearly_trends"] = yearly_trends.to_dict('records')

                    # Patrones estacionales
                    seasonal_patterns = df_with_dates.groupby(['category', 'season']).size().reset_index(name='count')
                    temporal_patterns["seasonal_patterns"] = seasonal_patterns.to_dict('records')

            except Exception as e:
                logger.warning(f"⚠️ Error en análisis temporal: {str(e)}")

        return temporal_patterns

    def _get_season(self, month: int) -> str:
        """
        Convierte mes a estación

        Args:
            month: Número del mes (1-12)

        Returns:
            Nombre de la estación
        """
        if month in [12, 1, 2]:
            return "Winter"
        elif month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        else:
            return "Fall"

    def extract_text_features(self, all_data: Dict[str, List[Dict]]) -> Dict:
        """
        Extrae características del texto de las reseñas

        Args:
            all_data: Datos de todas las categorías

        Returns:
            Diccionario con características de texto
        """
        text_features = {
            "word_counts": {},
            "sentiment_indicators": {},
            "common_phrases": {}
        }

        for category, data in all_data.items():
            category_texts = []

            for record in data:
                review_text = record.get('reviewText', '')
                if review_text:
                    category_texts.append(review_text.lower())

            if category_texts:
                # Contar palabras comunes
                all_words = []
                for text in category_texts:
                    # Limpieza básica
                    words = re.findall(r'\b\w+\b', text)
                    all_words.extend([word for word in words if len(word) > 3])

                # Palabras más comunes
                from collections import Counter
                word_counts = Counter(all_words)
                text_features["word_counts"][category] = dict(word_counts.most_common(20))

                # Indicadores de sentimiento básicos
                positive_words = ['great', 'excellent', 'amazing', 'perfect', 'love', 'best']
                negative_words = ['terrible', 'awful', 'worst', 'hate', 'horrible', 'bad']

                positive_count = sum(text.count(word) for text in category_texts for word in positive_words)
                negative_count = sum(text.count(word) for text in category_texts for word in negative_words)

                text_features["sentiment_indicators"][category] = {
                    "positive_mentions": positive_count,
                    "negative_mentions": negative_count,
                    "sentiment_ratio": positive_count / (negative_count + 1)  # Evitar división por 0
                }

        return text_features

    def create_sample_dataset(self, all_data: Dict[str, List[Dict]], sample_size: int = 100) -> List[Dict]:
        """
        Crea un dataset de muestra para entrega

        Args:
            all_data: Todos los datos
            sample_size: Tamaño de la muestra por categoría

        Returns:
            Lista con muestra representativa
        """
        sample_data = []

        for category, data in all_data.items():
            # Tomar muestra aleatoria
            category_sample = data[:min(sample_size, len(data))]

            # Agregar identificador de categoría
            for record in category_sample:
                record_copy = record.copy()
                record_copy['source_category'] = category
                sample_data.append(record_copy)

        # Guardar muestra
        sample_file = self.samples_dir / "representative_sample.json"
        with open(sample_file, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, indent=2, ensure_ascii=False)

        logger.info(f"📄 Muestra creada: {len(sample_data)} registros en {sample_file}")
        return sample_data


def main():
    """
    Función principal para probar el extractor
    """
    print("📊 Amazon Data Extractor")
    print("=" * 40)

    # Crear extractor
    extractor = AmazonDataExtractor()

    # Cargar todos los datos
    print("📥 Cargando datos...")
    all_data = extractor.load_all_data()

    if not all_data:
        print("❌ No se encontraron datos. Ejecuta primero el downloader.")
        return

    print(f"✅ Datos cargados: {len(all_data)} categorías")

    # Extraer estadísticas básicas
    print("\n📊 Extrayendo estadísticas básicas...")
    comparison = extractor.extract_category_comparison(all_data)

    # Mostrar resumen
    print("\n" + "=" * 50)
    print("📈 RESUMEN POR CATEGORÍA:")
    print("=" * 50)

    for category, stats in comparison["category_stats"].items():
        print(f"\n🏷️  {category}:")
        print(f"   📊 Registros: {stats.get('total_records', 0)}")
        print(f"   👥 Usuarios únicos: {stats.get('unique_users', 0)}")
        print(f"   🛍️  Productos únicos: {stats.get('unique_products', 0)}")
        print(f"   ⭐ Rating promedio: {stats.get('avg_rating', 0):.2f}")

    # Entertainment vs Home
    if "entertainment_vs_home" in comparison:
        print("\n" + "=" * 50)
        print("🎭 ENTERTAINMENT vs 🏠 HOME:")
        print("=" * 50)

        ent_stats = comparison["entertainment_vs_home"]["entertainment"]
        home_stats = comparison["entertainment_vs_home"]["home"]

        print(f"🎭 Entertainment: {ent_stats.get('total_records', 0)} registros")
        print(f"   ⭐ Rating promedio: {ent_stats.get('avg_rating', 0):.2f}")

        print(f"🏠 Home: {home_stats.get('total_records', 0)} registros")
        print(f"   ⭐ Rating promedio: {home_stats.get('avg_rating', 0):.2f}")

    # Usuarios cross-category
    cross_users = comparison.get("cross_category_users", {})
    print(f"\n🔗 Usuarios multi-categoría: {cross_users.get('total_cross_users', 0)}")
    print(f"   📊 Porcentaje: {cross_users.get('percentage', 0):.1f}%")

    # Crear muestra para entrega
    print("\n📄 Creando muestra representativa...")
    sample_data = extractor.create_sample_dataset(all_data, sample_size=50)
    print(f"✅ Muestra creada: {len(sample_data)} registros")

    print("\n✅ Extracción completada")
    print(f"📁 Revisa los archivos en: {extractor.samples_dir}")


if __name__ == "__main__":
    main()