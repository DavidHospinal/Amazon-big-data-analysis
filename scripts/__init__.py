#!/usr/bin/env python3
"""
Scripts de utilidad para el proyecto Amazon Big Data Analysis

Este módulo contiene scripts de mantenimiento, limpieza y utilidades
para el proyecto de análisis de reviews de Amazon.

Autor: Oscar David Hospinal R.
Curso: INF3590 - Big Data
Universidad: Pontificia Universidad Católica de Chile
"""

import sys
import os
from pathlib import Path

# Configuración del proyecto
PROJECT_NAME = "Amazon Big Data Analysis"
PROJECT_VERSION = "1.0.0"
AUTHOR = "Oscar David Hospinal R."


def get_project_root():
    """
    Obtiene la ruta raíz del proyecto automáticamente

    Returns:
        Path: Ruta absoluta al directorio raíz del proyecto
    """
    current_file = Path(__file__).resolve()
    # Subir desde scripts/ hasta la raíz del proyecto
    project_root = current_file.parent.parent
    return project_root


def setup_project_path():
    """
    Configura el PYTHONPATH para importar módulos del proyecto

    Permite importar desde src/, config/, etc. sin problemas
    """
    project_root = get_project_root()

    # Agregar rutas importantes al sys.path si no están
    paths_to_add = [
        str(project_root),  # Raíz del proyecto
        str(project_root / "src"),  # Código fuente
        str(project_root / "config"),  # Configuraciones
        str(project_root / "scripts"),  # Scripts (esta carpeta)
    ]

    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)


def validate_project_structure():
    """
    Valida que la estructura del proyecto sea correcta

    Returns:
        bool: True si la estructura es válida, False en caso contrario
    """
    project_root = get_project_root()

    required_dirs = ['src', 'data', 'config', 'notebooks', 'tests']
    required_files = ['requirements.txt', 'README.md']

    missing_items = []

    # Verificar directorios
    for directory in required_dirs:
        if not (project_root / directory).exists():
            missing_items.append(f"📁 {directory}/")

    # Verificar archivos
    for file in required_files:
        if not (project_root / file).exists():
            missing_items.append(f"📄 {file}")

    if missing_items:
        print("⚠️ Estructura del proyecto incompleta:")
        for item in missing_items:
            print(f"   ❌ Falta: {item}")
        return False

    print("✅ Estructura del proyecto válida")
    return True


def list_available_scripts():
    """
    Lista todos los scripts disponibles en esta carpeta

    Returns:
        list: Lista de archivos Python ejecutables
    """
    scripts_dir = Path(__file__).parent

    scripts = []
    for file in scripts_dir.glob("*.py"):
        if file.name != "__init__.py":
            scripts.append(file.name)

    return sorted(scripts)


def print_project_info():
    """Imprime información del proyecto y scripts disponibles"""

    print("=" * 60)
    print(f"🎯 {PROJECT_NAME}")
    print(f"📋 Versión: {PROJECT_VERSION}")
    print(f"👤 Autor: {AUTHOR}")
    print("=" * 60)

    project_root = get_project_root()
    print(f"📍 Proyecto ubicado en: {project_root}")

    # Validar estructura
    print("\n🔍 Validando estructura del proyecto...")
    validate_project_structure()

    # Listar scripts disponibles
    scripts = list_available_scripts()
    if scripts:
        print(f"\n🛠️ Scripts disponibles ({len(scripts)}):")
        for i, script in enumerate(scripts, 1):
            print(f"   {i}. {script}")

        print(f"\n💡 Para ejecutar un script:")
        print(f"   python scripts/{scripts[0] if scripts else 'script_name.py'}")
    else:
        print("\n📝 No hay scripts adicionales disponibles")

    print("\n" + "=" * 60)


# Configurar automáticamente cuando se importa el módulo
setup_project_path()

# Si se ejecuta directamente este archivo
if __name__ == "__main__":
    print("🚀 Iniciando módulo de scripts...")
    print_project_info()

    # Mostrar menú interactivo si hay scripts disponibles
    scripts = list_available_scripts()

    if scripts:
        print("\n🎮 ¿Quieres ejecutar algún script?")
        print("0. Salir")

        for i, script in enumerate(scripts, 1):
            script_name = script.replace('.py', '').replace('_', ' ').title()
            print(f"{i}. {script_name}")

        try:
            choice = input("\n👉 Selecciona una opción (0-{}): ".format(len(scripts)))
            choice = int(choice)

            if choice == 0:
                print("👋 ¡Hasta luego!")
            elif 1 <= choice <= len(scripts):
                selected_script = scripts[choice - 1]
                print(f"\n🏃‍♂️ Ejecutando {selected_script}...")
                print("-" * 40)

                # Importar y ejecutar el script seleccionado
                script_module = selected_script.replace('.py', '')
                try:
                    exec(f"import {script_module}")
                    exec(f"{script_module}.main()")
                except Exception as e:
                    print(f"❌ Error al ejecutar {selected_script}: {e}")
                    print("💡 Intenta ejecutarlo directamente:")
                    print(f"   python scripts/{selected_script}")
            else:
                print("❌ Opción inválida")

        except ValueError:
            print("❌ Por favor ingresa un número válido")
        except KeyboardInterrupt:
            print("\n👋 ¡Hasta luego!")

    else:
        print("\n👋 No hay scripts para ejecutar interactivamente")