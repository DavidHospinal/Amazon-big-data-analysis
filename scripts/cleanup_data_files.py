#!/usr/bin/env python3
"""
Script para limpiar y optimizar la estructura del proyecto Amazon Big Data
Elimina archivos redundantes y mantiene solo los necesarios
"""

import os
import json
from pathlib import Path
import shutil
from datetime import datetime


def analyze_json_files(project_root):
    """Analiza los archivos JSON para decidir cuáles mantener"""

    data_dir = Path(project_root) / "data"

    files_info = {}

    # Buscar todos los archivos amazon_reviews*.json
    for json_file in data_dir.glob("amazon_reviews*.json"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Obtener información del archivo
            file_size = json_file.stat().st_size
            num_records = len(data) if isinstance(data, list) else len(data.get('reviews', []))

            files_info[str(json_file)] = {
                'size_mb': round(file_size / (1024 * 1024), 2),
                'records': num_records,
                'modified': datetime.fromtimestamp(json_file.stat().st_mtime),
                'has_data': num_records > 0
            }

        except Exception as e:
            files_info[str(json_file)] = {
                'error': str(e),
                'size_mb': round(json_file.stat().st_size / (1024 * 1024), 2),
                'has_data': False
            }

    return files_info


def recommend_cleanup(files_info):
    """Recomienda qué archivos mantener y cuáles eliminar"""

    recommendations = {
        'keep': [],
        'delete': [],
        'backup': []
    }

    main_file = None
    backup_files = []

    for filepath, info in files_info.items():
        filename = Path(filepath).name

        if filename == "amazon_reviews.json":
            main_file = (filepath, info)
        elif "amazon_reviews_" in filename and filename.endswith(".json"):
            backup_files.append((filepath, info))

    # Decidir qué hacer con el archivo principal
    if main_file:
        if main_file[1].get('has_data', False):
            recommendations['keep'].append({
                'file': main_file[0],
                'reason': 'Archivo principal con datos válidos',
                'info': main_file[1]
            })
        else:
            recommendations['delete'].append({
                'file': main_file[0],
                'reason': 'Archivo principal vacío o corrupto',
                'info': main_file[1]
            })

    # Decidir qué hacer con los backups
    if backup_files:
        # Ordenar por fecha de modificación (más reciente primero)
        backup_files.sort(key=lambda x: x[1].get('modified', datetime.min), reverse=True)

        for i, (filepath, info) in enumerate(backup_files):
            if i == 0 and info.get('has_data', False):
                # Mantener el backup más reciente con datos
                if not main_file or not main_file[1].get('has_data', False):
                    recommendations['keep'].append({
                        'file': filepath,
                        'reason': 'Backup más reciente con datos (convertir a principal)',
                        'info': info,
                        'action': 'rename_to_main'
                    })
                else:
                    recommendations['backup'].append({
                        'file': filepath,
                        'reason': 'Backup reciente (mover a carpeta backup)',
                        'info': info
                    })
            else:
                recommendations['delete'].append({
                    'file': filepath,
                    'reason': 'Backup antiguo o redundante',
                    'info': info
                })

    return recommendations


def execute_cleanup(recommendations, project_root, dry_run=True):
    """Ejecuta la limpieza según las recomendaciones"""

    print(f"{'DRY RUN - ' if dry_run else ''}Ejecutando limpieza...")

    # Crear carpeta de backup si es necesaria
    backup_dir = Path(project_root) / "data" / "backup"

    if recommendations['backup'] and not dry_run:
        backup_dir.mkdir(exist_ok=True)

    actions_taken = []

    # Procesar archivos a mantener
    for item in recommendations['keep']:
        if item.get('action') == 'rename_to_main':
            old_path = Path(item['file'])
            new_path = old_path.parent / "amazon_reviews.json"

            if not dry_run:
                if new_path.exists():
                    new_path.unlink()  # Eliminar el archivo principal corrupto
                old_path.rename(new_path)

            actions_taken.append(f"RENAMED: {old_path.name} → amazon_reviews.json")
        else:
            actions_taken.append(f"KEPT: {Path(item['file']).name}")

    # Procesar archivos a hacer backup
    for item in recommendations['backup']:
        old_path = Path(item['file'])
        new_path = backup_dir / old_path.name

        if not dry_run:
            shutil.move(str(old_path), str(new_path))

        actions_taken.append(f"MOVED TO BACKUP: {old_path.name}")

    # Procesar archivos a eliminar
    for item in recommendations['delete']:
        file_path = Path(item['file'])

        if not dry_run:
            file_path.unlink()

        actions_taken.append(f"DELETED: {file_path.name} ({item['reason']})")

    return actions_taken


def main():
    """Función principal"""

    # Detectar automáticamente la ruta del proyecto
    script_dir = Path(__file__).parent
    project_root = script_dir.parent  # Subir un nivel desde scripts/

    print(f"🎯 Proyecto detectado en: {project_root}")

    # Verificar que estamos en el directorio correcto
    if not (project_root / "data").exists():
        print("❌ Error: No se encontró la carpeta 'data'")
        print("   Asegúrate de ejecutar desde la carpeta 'scripts' del proyecto")
        return

    print("🔍 ANALIZANDO ARCHIVOS JSON...")
    print("=" * 50)

    # Analizar archivos
    files_info = analyze_json_files(project_root)

    if not files_info:
        print("❌ No se encontraron archivos amazon_reviews*.json")
        return

    # Mostrar información de archivos encontrados
    print("📁 ARCHIVOS ENCONTRADOS:")
    for filepath, info in files_info.items():
        filename = Path(filepath).name
        if 'error' in info:
            print(f"  ❌ {filename}: ERROR - {info['error']}")
        else:
            print(f"  📄 {filename}:")
            print(f"     Size: {info['size_mb']} MB")
            print(f"     Records: {info['records']}")
            print(f"     Modified: {info['modified']}")
            print(f"     Has Data: {'✅' if info['has_data'] else '❌'}")
        print()

    # Generar recomendaciones
    recommendations = recommend_cleanup(files_info)

    print("💡 RECOMENDACIONES:")
    print("=" * 50)

    if recommendations['keep']:
        print("✅ MANTENER:")
        for item in recommendations['keep']:
            action_text = f" ({item.get('action', 'keep as-is')})" if item.get('action') else ""
            print(f"  • {Path(item['file']).name}{action_text}")
            print(f"    Razón: {item['reason']}")

    if recommendations['backup']:
        print("\n📦 MOVER A BACKUP:")
        for item in recommendations['backup']:
            print(f"  • {Path(item['file']).name}")
            print(f"    Razón: {item['reason']}")

    if recommendations['delete']:
        print("\n🗑️ ELIMINAR:")
        for item in recommendations['delete']:
            print(f"  • {Path(item['file']).name}")
            print(f"    Razón: {item['reason']}")

    # Ejecutar dry run
    print("\n🧪 SIMULACIÓN (DRY RUN):")
    print("=" * 50)
    actions = execute_cleanup(recommendations, project_root, dry_run=False)
    for action in actions:
        print(f"  {action}")

    print("\n" + "=" * 50)
    print("✅ LIMPIEZA EJECUTADA - Archivos organizados correctamente")


if __name__ == "__main__":
    main()