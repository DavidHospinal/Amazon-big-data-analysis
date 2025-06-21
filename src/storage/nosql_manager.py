"""
Amazon Reviews NoSQL Database Manager
====================================
Gestiona almacenamiento y consultas en base de datos NoSQL (TinyDB/MongoDB)

Autor: [Tu Nombre]
Proyecto: Amazon Big Data Analysis
Curso: INF3590 - Big Data
"""

from tinydb import TinyDB, Query
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class NoSQLManager:
    """
    Gestor de base de datos NoSQL para reseñas de Amazon
    """

    def __init__(self, db_type: str = "tinydb", db_path: str = "../../data/amazon_reviews.json"):
        """
        Inicializa el gestor NoSQL

        Args:
            db_type: Tipo de BD ('tinydb' o 'mongodb')
            db_path: Ruta de la base de datos
        """
        self.db_type = db_type
        self.script_dir = Path(__file__).parent
        self.db_path = (self.script_dir / db_path).resolve()

        # Asegurar que el directorio existe
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        if db_type == "tinydb":
            self._init_tinydb()
        else:
            raise NotImplementedError("Solo TinyDB implementado por ahora")

        logger.info(f"📊 NoSQL Manager inicializado: {db_type}")
        logger.info(f"📁 Base de datos: {self.db_path}")

    def _init_tinydb(self):
        """Inicializa TinyDB con configuración optimizada"""
        try:
            # TinyDB con caché para mejor rendimiento
            self.db = TinyDB(
                str(self.db_path),
                storage=CachingMiddleware(JSONStorage),
                sort_keys=True,
                indent=2,
                ensure_ascii=False
            )

            # Crear tablas por categoría
            self.tables = {
                'reviews': self.db.table('reviews'),
                'books': self.db.table('books'),
                'video_games': self.db.table('video_games'),
                'movies_tv': self.db.table('movies_tv'),
                'home_kitchen': self.db.table('home_kitchen'),
                'tools': self.db.table('tools'),
                'patio_garden': self.db.table('patio_garden'),
                'metadata': self.db.table('metadata')
            }

            logger.info("✅ TinyDB inicializado correctamente")

        except Exception as e:
            logger.error(f"❌ Error inicializando TinyDB: {str(e)}")
            raise

    def insert_reviews(self, data: List[Dict], category: str = None) -> bool:
        """
        Inserta reseñas en la base de datos

        Args:
            data: Lista de reseñas
            category: Categoría específica (opcional)

        Returns:
            True si la inserción fue exitosa
        """
        try:
            if not data:
                logger.warning("⚠️ No hay datos para insertar")
                return False

            # Preparar datos para inserción
            processed_data = []
            for record in data:
                # Agregar metadata de inserción
                processed_record = record.copy()
                processed_record['inserted_at'] = datetime.now().isoformat()
                processed_record['db_id'] = f"{record.get('reviewerID', 'unknown')}_{record.get('asin', 'unknown')}"

                processed_data.append(processed_record)

            # Insertar en tabla general de reviews
            self.tables['reviews'].insert_multiple(processed_data)

            # Insertar en tabla específica de categoría si se especifica
            if category:
                table_name = self._get_table_name(category)
                if table_name in self.tables:
                    self.tables[table_name].insert_multiple(processed_data)

            logger.info(f"✅ Insertados {len(processed_data)} registros")
            if category:
                logger.info(f"📋 Categoría: {category}")

            return True

        except Exception as e:
            logger.error(f"❌ Error insertando datos: {str(e)}")
            return False

    def load_all_categories(self) -> bool:
        """
        Carga todas las categorías desde archivos procesados

        Returns:
            True si la carga fue exitosa
        """
        categories = {
            'Books': 'books',
            'Video_Games': 'video_games',
            'Movies_and_TV': 'movies_tv',
            'Home_and_Kitchen': 'home_kitchen',
            'Tools_and_Home_Improvement': 'tools',
            'Patio_Lawn_and_Garden': 'patio_garden'
        }

        processed_dir = self.db_path.parent / "processed"
        total_inserted = 0

        logger.info("📥 Cargando todas las categorías a NoSQL...")

        for category_file, table_name in categories.items():
            file_path = processed_dir / f"{category_file}_sample.json"

            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # Insertar datos
                    success = self.insert_reviews(data, category_file)
                    if success:
                        total_inserted += len(data)
                        logger.info(f"✅ {category_file}: {len(data)} registros")
                    else:
                        logger.warning(f"⚠️ Error cargando {category_file}")

                except Exception as e:
                    logger.error(f"❌ Error leyendo {category_file}: {str(e)}")
            else:
                logger.warning(f"⚠️ Archivo no encontrado: {file_path}")

        # Guardar metadata de carga
        self._save_load_metadata(total_inserted, len(categories))

        logger.info(f"🎉 Carga completada: {total_inserted} registros totales")
        return total_inserted > 0

    def _get_table_name(self, category: str) -> str:
        """Convierte nombre de categoría a nombre de tabla"""
        mapping = {
            'Books': 'books',
            'Video_Games': 'video_games',
            'Movies_and_TV': 'movies_tv',
            'Home_and_Kitchen': 'home_kitchen',
            'Tools_and_Home_Improvement': 'tools',
            'Patio_Lawn_and_Garden': 'patio_garden'
        }
        return mapping.get(category, 'reviews')

    def _save_load_metadata(self, total_records: int, categories_count: int):
        """Guarda metadata de la carga"""
        metadata = {
            'load_timestamp': datetime.now().isoformat(),
            'total_records': total_records,
            'categories_loaded': categories_count,
            'db_type': self.db_type,
            'db_path': str(self.db_path)
        }

        self.tables['metadata'].insert(metadata)

    def get_basic_stats(self) -> Dict:
        """
        Obtiene estadísticas básicas de la base de datos

        Returns:
            Diccionario con estadísticas
        """
        try:
            stats = {
                'total_reviews': len(self.tables['reviews']),
                'categories': {},
                'database_info': {
                    'type': self.db_type,
                    'path': str(self.db_path),
                    'size_mb': round(self.db_path.stat().st_size / (1024 * 1024), 2) if self.db_path.exists() else 0
                }
            }

            # Stats por categoría
            category_tables = ['books', 'video_games', 'movies_tv', 'home_kitchen', 'tools', 'patio_garden']
            for table_name in category_tables:
                if table_name in self.tables:
                    count = len(self.tables[table_name])
                    stats['categories'][table_name] = count

            return stats

        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas: {str(e)}")
            return {}

    def query_by_rating(self, min_rating: float, max_rating: float = 5.0, category: str = None) -> List[Dict]:
        """
        Consulta de filtrado por rating

        Args:
            min_rating: Rating mínimo
            max_rating: Rating máximo
            category: Categoría específica (opcional)

        Returns:
            Lista de reseñas que cumplen el criterio
        """
        try:
            Review = Query()
            query_condition = (Review.overall >= min_rating) & (Review.overall <= max_rating)

            # Seleccionar tabla
            if category:
                table_name = self._get_table_name(category)
                table = self.tables.get(table_name, self.tables['reviews'])
            else:
                table = self.tables['reviews']

            results = table.search(query_condition)

            logger.info(f"🔍 Consulta por rating [{min_rating}-{max_rating}]: {len(results)} resultados")
            if category:
                logger.info(f"📋 Categoría: {category}")

            return results

        except Exception as e:
            logger.error(f"❌ Error en consulta por rating: {str(e)}")
            return []

    def aggregate_by_category(self) -> Dict[str, Dict]:
        """
        Consulta de agregación: estadísticas por categoría

        Returns:
            Diccionario con agregaciones por categoría
        """
        try:
            aggregations = {}

            category_mapping = {
                'books': 'Books',
                'video_games': 'Video Games',
                'movies_tv': 'Movies & TV',
                'home_kitchen': 'Home & Kitchen',
                'tools': 'Tools & Home Improvement',
                'patio_garden': 'Patio, Lawn & Garden'
            }

            for table_name, display_name in category_mapping.items():
                if table_name in self.tables:
                    records = self.tables[table_name].all()

                    if records:
                        # Convertir a DataFrame para agregaciones fáciles
                        df = pd.DataFrame(records)

                        agg_data = {
                            'count': len(records),
                            'avg_rating': df['overall'].mean() if 'overall' in df.columns else 0,
                            'min_rating': df['overall'].min() if 'overall' in df.columns else 0,
                            'max_rating': df['overall'].max() if 'overall' in df.columns else 0,
                            'unique_users': df['reviewerID'].nunique() if 'reviewerID' in df.columns else 0,
                            'unique_products': df['asin'].nunique() if 'asin' in df.columns else 0
                        }

                        # Distribución de ratings
                        if 'overall' in df.columns:
                            rating_dist = df['overall'].value_counts().sort_index().to_dict()
                            agg_data['rating_distribution'] = rating_dist

                        aggregations[display_name] = agg_data

            logger.info(f"📊 Agregación completada: {len(aggregations)} categorías")
            return aggregations

        except Exception as e:
            logger.error(f"❌ Error en agregación: {str(e)}")
            return {}

    def query_top_products(self, category: str = None, limit: int = 10) -> List[Dict]:
        """
        Consulta: productos mejor valorados

        Args:
            category: Categoría específica (opcional)
            limit: Número máximo de resultados

        Returns:
            Lista de productos top
        """
        try:
            # Seleccionar tabla
            if category:
                table_name = self._get_table_name(category)
                table = self.tables.get(table_name, self.tables['reviews'])
            else:
                table = self.tables['reviews']

            records = table.all()

            if not records:
                return []

            # Convertir a DataFrame y agrupar por producto
            df = pd.DataFrame(records)

            if 'asin' not in df.columns or 'overall' not in df.columns:
                return []

            # Agrupar por producto y calcular rating promedio
            product_ratings = df.groupby('asin').agg({
                'overall': ['mean', 'count'],
                'reviewText': 'first'  # Para obtener info del producto
            }).round(2)

            # Aplanar columnas
            product_ratings.columns = ['avg_rating', 'review_count', 'sample_review']
            product_ratings = product_ratings.reset_index()

            # Ordenar por rating y filtrar productos con múltiples reseñas
            top_products = product_ratings[product_ratings['review_count'] >= 2].sort_values(
                'avg_rating', ascending=False
            ).head(limit)

            results = top_products.to_dict('records')

            logger.info(f"🏆 Top productos encontrados: {len(results)}")
            if category:
                logger.info(f"📋 Categoría: {category}")

            return results

        except Exception as e:
            logger.error(f"❌ Error obteniendo top productos: {str(e)}")
            return []

    def close(self):
        """Cierra la conexión a la base de datos"""
        try:
            if hasattr(self, 'db'):
                self.db.close()
            logger.info("✅ Base de datos cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error cerrando base de datos: {str(e)}")


def main():
    """
    Función principal para probar el gestor NoSQL
    """
    print("🗄️ Amazon NoSQL Manager")
    print("=" * 40)

    # Crear gestor
    nosql = NoSQLManager()

    # Cargar todas las categorías
    print("📥 Cargando datos a NoSQL...")
    success = nosql.load_all_categories()

    if not success:
        print("❌ Error cargando datos")
        return

    # Mostrar estadísticas básicas
    print("\n📊 Estadísticas básicas:")
    stats = nosql.get_basic_stats()

    print(f"📈 Total reseñas: {stats.get('total_reviews', 0)}")
    print(f"💾 Tamaño BD: {stats.get('database_info', {}).get('size_mb', 0)} MB")

    print("\n📋 Por categoría:")
    for category, count in stats.get('categories', {}).items():
        print(f"   {category}: {count} registros")

    # Ejemplo de consulta de filtrado
    print("\n🔍 CONSULTA DE FILTRADO:")
    print("Reseñas con rating >= 4.5:")
    high_rated = nosql.query_by_rating(4.5)
    print(f"✅ Encontradas: {len(high_rated)} reseñas")

    # Ejemplo de consulta de agregación
    print("\n📊 CONSULTA DE AGREGACIÓN:")
    print("Estadísticas por categoría:")
    aggregations = nosql.aggregate_by_category()

    for category, data in aggregations.items():
        print(f"\n🏷️ {category}:")
        print(f"   📊 Total: {data.get('count', 0)}")
        print(f"   ⭐ Rating promedio: {data.get('avg_rating', 0):.2f}")
        print(f"   👥 Usuarios únicos: {data.get('unique_users', 0)}")

    print("\n✅ Pruebas completadas")

    # Cerrar conexión
    nosql.close()


if __name__ == "__main__":
    main()