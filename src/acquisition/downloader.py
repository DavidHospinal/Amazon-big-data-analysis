"""
Amazon Reviews Data Downloader
============================
Descarga datasets de reseñas de Amazon desde Stanford SNAP
Categorías: Entertainment + Home Products (6 categorías total)

Autor: [Oscar David Hospinal R.]
Proyecto: Amazon Big Data Analysis
Curso: INF3590 - Big Data
"""

import requests
import gzip
import json
import time
from pathlib import Path
from typing import List, Dict, Optional
from tqdm import tqdm
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AmazonDataDownloader:
    """
    Descargador de datos de Amazon Reviews desde Stanford SNAP
    """

    def __init__(self, base_data_dir: str = "../../data"):
        """
        Inicializa el downloader

        Args:
            base_data_dir: Directorio base para datos (relativo a src/acquisition)
        """
        # Crear rutas absolutas desde la ubicación del script
        self.script_dir = Path(__file__).parent
        self.base_dir = (self.script_dir / base_data_dir).resolve()
        self.raw_dir = self.base_dir / "raw"
        self.processed_dir = self.base_dir / "processed"
        self.samples_dir = self.base_dir / "samples"

        # Crear directorios si no existen
        for directory in [self.raw_dir, self.processed_dir, self.samples_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        logger.info(f"📁 Directorios configurados en: {self.base_dir}")

        # Configuración de categorías
        self.categories = {
            # ENTRETENIMIENTO (600 registros)
            "Books": {
                "url": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Books_5.json.gz",
                "sample_size": 200,
                "category_group": "Entertainment",
                "analysis_type": "Leisure/Personal",
                "priority": 1
            },
            "Video_Games": {
                "url": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Video_Games_5.json.gz",
                "sample_size": 200,
                "category_group": "Entertainment",
                "analysis_type": "Digital/Interactive",
                "priority": 2
            },
            "Movies_and_TV": {
                "url": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Movies_and_TV_5.json.gz",
                "sample_size": 200,
                "category_group": "Entertainment",
                "analysis_type": "Visual/Passive",
                "priority": 3
            },

            # HOGAR (1500 registros)
            "Home_and_Kitchen": {
                "url": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Home_and_Kitchen_5.json.gz",
                "sample_size": 200,
                "category_group": "Home",
                "analysis_type": "Essential/Daily",
                "priority": 4
            },
            "Tools_and_Home_Improvement": {
                "url": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Tools_and_Home_Improvement_5.json.gz",
                "sample_size": 200,
                "category_group": "Home",
                "analysis_type": "Functional/Project",
                "priority": 5
            },
            "Patio_Lawn_and_Garden": {
                "url": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Patio_Lawn_and_Garden_5.json.gz",
                "sample_size": 200,
                "category_group": "Home",
                "analysis_type": "Outdoor/Seasonal",
                "priority": 6
            }
        }

    def download_category(self, category_name: str, force_redownload: bool = False) -> Optional[List[Dict]]:
        """
        Descarga una categoría específica

        Args:
            category_name: Nombre de la categoría
            force_redownload: Si True, descarga aunque el archivo ya exista

        Returns:
            Lista de registros procesados o None si hay error
        """
        if category_name not in self.categories:
            logger.error(f"❌ Categoría '{category_name}' no encontrada")
            return None

        config = self.categories[category_name]
        url = config["url"]
        sample_size = config["sample_size"]

        # Archivos de destino
        gz_file = self.raw_dir / f"{category_name}.json.gz"
        sample_file = self.processed_dir / f"{category_name}_sample.json"

        # Verificar si ya existe (a menos que sea forzado)
        if not force_redownload and sample_file.exists():
            logger.info(f"✅ {category_name} ya existe, cargando desde archivo...")
            with open(sample_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        logger.info(f"📥 Descargando {category_name}...")
        logger.info(f"🔗 URL: {url}")

        try:
            # Descarga con barra de progreso
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Obtener tamaño del archivo
            total_size = int(response.headers.get('content-length', 0))

            # Descargar con progreso
            with open(gz_file, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Descargando {category_name}") as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            logger.info(f"✅ Descarga completada: {gz_file}")

            # Extraer y procesar registros
            records = self._extract_records(gz_file, sample_size, category_name)

            if records:
                # Enriquecer datos con metadata de categoría
                enriched_records = self._enrich_records(records, config)

                # Guardar muestra procesada
                with open(sample_file, 'w', encoding='utf-8') as f:
                    json.dump(enriched_records, f, indent=2, ensure_ascii=False)

                logger.info(f"✅ {category_name}: {len(enriched_records)} registros guardados")
                logger.info(f"📁 Archivo: {sample_file}")

                return enriched_records
            else:
                logger.error(f"❌ No se pudieron extraer registros de {category_name}")
                return None

        except Exception as e:
            logger.error(f"❌ Error descargando {category_name}: {str(e)}")
            return None

    def _extract_records(self, gz_file: Path, max_records: int, category_name: str) -> List[Dict]:
        """
        Extrae registros del archivo comprimido

        Args:
            gz_file: Archivo .gz a procesar
            max_records: Máximo número de registros
            category_name: Nombre de la categoría (para logging)

        Returns:
            Lista de registros extraídos
        """
        records = []
        errors = 0

        logger.info(f"📖 Extrayendo registros de {category_name}...")

        try:
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                with tqdm(total=max_records, desc=f"Procesando {category_name}") as pbar:
                    for i, line in enumerate(f):
                        if len(records) >= max_records:
                            break

                        try:
                            record = json.loads(line.strip())

                            # Validar que el registro tenga campos mínimos
                            if self._validate_record(record):
                                records.append(record)
                                pbar.update(1)
                            else:
                                errors += 1

                        except json.JSONDecodeError:
                            errors += 1
                            continue
                        except Exception as e:
                            errors += 1
                            continue

            logger.info(f"📊 {category_name}: {len(records)} registros válidos, {errors} errores")
            return records

        except Exception as e:
            logger.error(f"❌ Error extrayendo {category_name}: {str(e)}")
            return []

    def _validate_record(self, record: Dict) -> bool:
        """
        Valida que un registro tenga los campos mínimos requeridos

        Args:
            record: Registro a validar

        Returns:
            True si es válido, False en caso contrario
        """
        required_fields = ['reviewerID', 'asin', 'overall', 'reviewTime']
        return all(field in record for field in required_fields)

    def _enrich_records(self, records: List[Dict], config: Dict) -> List[Dict]:
        """
        Enriquece registros con metadata de categoría

        Args:
            records: Lista de registros originales
            config: Configuración de la categoría

        Returns:
            Lista de registros enriquecidos
        """
        for record in records:
            record['category_group'] = config['category_group']
            record['analysis_type'] = config['analysis_type']
            record['download_timestamp'] = time.time()

        return records

    def download_all_categories(self, force_redownload: bool = False) -> Dict[str, List[Dict]]:
        """
        Descarga todas las categorías configuradas

        Args:
            force_redownload: Si True, re-descarga aunque existan archivos

        Returns:
            Diccionario con datos de todas las categorías
        """
        logger.info("🚀 Iniciando descarga de todas las categorías...")
        logger.info(f"📊 Total categorías: {len(self.categories)}")

        all_data = {}
        entertainment_total = 0
        home_total = 0

        # Ordenar por prioridad
        sorted_categories = sorted(
            self.categories.items(),
            key=lambda x: x[1]['priority']
        )

        for category_name, config in sorted_categories:
            logger.info(f"\n{'=' * 50}")
            logger.info(f"📦 Procesando: {category_name}")
            logger.info(f"🏷️  Grupo: {config['category_group']}")
            logger.info(f"🎯 Objetivo: {config['sample_size']} registros")

            # Descargar categoría
            data = self.download_category(category_name, force_redownload)

            if data:
                all_data[category_name] = data

                # Contar por grupo
                if config['category_group'] == 'Entertainment':
                    entertainment_total += len(data)
                else:
                    home_total += len(data)

                # Pausa entre descargas para ser respetuoso
                if category_name != sorted_categories[-1][0]:  # No pausar en el último
                    logger.info("⏸️  Pausa de 3 segundos...")
                    time.sleep(3)
            else:
                logger.warning(f"⚠️  Falló la descarga de {category_name}")

        # Resumen final
        total_records = sum(len(data) for data in all_data.values())

        logger.info(f"\n{'=' * 60}")
        logger.info("🎉 DESCARGA COMPLETADA")
        logger.info(f"📊 RESUMEN:")
        logger.info(f"   🎭 Entertainment: {entertainment_total} registros")
        logger.info(f"   🏠 Home: {home_total} registros")
        logger.info(f"   📈 Total: {total_records} registros")
        logger.info(f"   ✅ Categorías exitosas: {len(all_data)}/{len(self.categories)}")
        logger.info(f"📁 Archivos en: {self.processed_dir}")

        # Guardar resumen consolidado
        self._save_summary(all_data, entertainment_total, home_total, total_records)

        return all_data

    def _save_summary(self, all_data: Dict, entertainment_total: int, home_total: int, total_records: int):
        """
        Guarda un resumen consolidado de la descarga

        Args:
            all_data: Todos los datos descargados
            entertainment_total: Total registros entretenimiento
            home_total: Total registros hogar
            total_records: Total general
        """
        summary = {
            "download_summary": {
                "timestamp": time.time(),
                "total_records": total_records,
                "categories_downloaded": len(all_data),
                "entertainment_records": entertainment_total,
                "home_records": home_total
            },
            "category_breakdown": {}
        }

        for category, data in all_data.items():
            config = self.categories[category]
            summary["category_breakdown"][category] = {
                "records_downloaded": len(data),
                "target_records": config["sample_size"],
                "category_group": config["category_group"],
                "analysis_type": config["analysis_type"]
            }

        summary_file = self.samples_dir / "download_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info(f"📋 Resumen guardado en: {summary_file}")


def main():
    """
    Función principal para ejecutar la descarga
    """
    print("🛒 Amazon Reviews Data Downloader")
    print("=" * 50)

    # Crear downloader
    downloader = AmazonDataDownloader()

    # Opción: descargar todas las categorías
    choice = input("¿Descargar todas las categorías? (y/n): ").lower()

    if choice == 'y':
        # Descargar todo
        all_data = downloader.download_all_categories()

        if all_data:
            print(f"\n✅ Descarga exitosa: {sum(len(data) for data in all_data.values())} registros")
            print(f"📁 Revisa los archivos en: {downloader.processed_dir}")
        else:
            print("\n❌ La descarga falló")
    else:
        # Descargar categoría específica
        print("\nCategorías disponibles:")
        for i, category in enumerate(downloader.categories.keys(), 1):
            print(f"{i}. {category}")

        try:
            choice_num = int(input("Selecciona número de categoría: ")) - 1
            category_names = list(downloader.categories.keys())
            selected_category = category_names[choice_num]

            data = downloader.download_category(selected_category)
            if data:
                print(f"✅ Descarga exitosa: {len(data)} registros de {selected_category}")
            else:
                print(f"❌ Error descargando {selected_category}")

        except (ValueError, IndexError):
            print("❌ Selección inválida")


if __name__ == "__main__":
    main()