"""
Módulo de visualización de datos
Generación de gráficos estáticos e interactivos para análisis de reviews
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path


class DataVisualizer:
    """Clase para visualización de datos de reviews"""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        """
        Inicializa el visualizador

        Args:
            data: DataFrame con los datos a visualizar
        """
        self.data = data
        self.logger = logging.getLogger(__name__)
        self._setup_matplotlib()
        self._setup_plotly()

    def _setup_matplotlib(self):
        """Configuración de matplotlib"""
        plt.style.use('default')
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
        sns.set_palette("husl")

    def _setup_plotly(self):
        """Configuración de plotly"""
        self.plotly_colors = px.colors.qualitative.Set3
        self.plotly_template = "plotly_white"

    def load_data(self, data: pd.DataFrame) -> None:
        """
        Carga datos para visualización

        Args:
            data: DataFrame con los datos
        """
        self.data = data
        self.logger.info(f"Datos cargados para visualización: {len(data)} registros")

    def plot_rating_distribution(self, save_path: Optional[str] = None) -> plt.Figure:
        """
        Genera histograma de distribución de ratings

        Args:
            save_path: Ruta para guardar el gráfico

        Returns:
            Figura de matplotlib
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        fig, ax = plt.subplots(figsize=(10, 6))

        # Histograma
        ax.hist(self.data['overall'], bins=20, alpha=0.7, color='skyblue',
                edgecolor='black', density=False)

        # Líneas de estadísticas
        mean_rating = self.data['overall'].mean()
        median_rating = self.data['overall'].median()

        ax.axvline(mean_rating, color='red', linestyle='--', linewidth=2,
                   label=f'Promedio: {mean_rating:.2f}⭐')
        ax.axvline(median_rating, color='green', linestyle='--', linewidth=2,
                   label=f'Mediana: {median_rating:.1f}⭐')

        ax.set_title('Distribución de Ratings - Amazon Reviews', fontsize=14, fontweight='bold')
        ax.set_xlabel('Rating (⭐)')
        ax.set_ylabel('Frecuencia')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Agregar texto con estadísticas
        stats_text = f'Total: {len(self.data):,} reviews\nDesv. Std: {self.data["overall"].std():.3f}'
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')

        return fig

    def plot_satisfaction_levels(self, save_path: Optional[str] = None) -> plt.Figure:
        """
        Genera gráfico de niveles de satisfacción

        Args:
            save_path: Ruta para guardar el gráfico

        Returns:
            Figura de matplotlib
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        # Crear niveles de satisfacción
        satisfaction_levels = pd.cut(
            self.data['overall'],
            bins=[0, 2.5, 3.5, 4.5, 5.1],
            labels=['Baja (≤2.5)', 'Media (2.6-3.5)', 'Buena (3.6-4.5)', 'Excelente (>4.5)']
        )

        satisfaction_counts = satisfaction_levels.value_counts()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Gráfico de barras
        colors = ['red', 'orange', 'lightblue', 'darkgreen']
        bars = ax1.bar(range(len(satisfaction_counts)), satisfaction_counts.values,
                       color=colors, alpha=0.7, edgecolor='black')

        ax1.set_title('Distribución por Niveles de Satisfacción')
        ax1.set_xlabel('Nivel de Satisfacción')
        ax1.set_ylabel('Número de Reviews')
        ax1.set_xticks(range(len(satisfaction_counts)))
        ax1.set_xticklabels(satisfaction_counts.index, rotation=45)
        ax1.grid(True, alpha=0.3)

        # Agregar valores en las barras
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width() / 2., height + 5,
                     f'{int(height)}\n({height / len(self.data) * 100:.1f}%)',
                     ha='center', va='bottom')

        # Gráfico circular
        wedges, texts, autotexts = ax2.pie(satisfaction_counts.values,
                                           labels=satisfaction_counts.index,
                                           colors=colors,
                                           autopct='%1.1f%%',
                                           startangle=90)
        ax2.set_title('Distribución Porcentual de Satisfacción')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')

        return fig

    def plot_category_comparison(self, save_path: Optional[str] = None) -> plt.Figure:
        """
        Genera comparación por categorías

        Args:
            save_path: Ruta para guardar el gráfico

        Returns:
            Figura de matplotlib
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        if 'original_category' not in self.data.columns:
            raise ValueError("No hay información de categorías")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Análisis Comparativo por Categorías', fontsize=16, fontweight='bold')

        # 1. Box plot por categorías
        category_order = self.data.groupby('original_category')['overall'].mean().sort_values(ascending=False).index
        df_ordered = self.data.set_index('original_category').loc[category_order].reset_index()

        sns.boxplot(data=df_ordered, y='original_category', x='overall', ax=axes[0, 0], palette='Set2')
        axes[0, 0].set_title('Distribución de Ratings por Categoría')
        axes[0, 0].set_xlabel('Rating (⭐)')
        axes[0, 0].grid(True, alpha=0.3)

        # 2. Barras de rating promedio
        category_means = self.data.groupby('original_category')['overall'].mean().sort_values(ascending=True)
        bars = axes[0, 1].barh(range(len(category_means)), category_means.values,
                               color=plt.cm.RdYlGn(category_means.values / 5), alpha=0.8)
        axes[0, 1].set_title('Rating Promedio por Categoría')
        axes[0, 1].set_xlabel('Rating Promedio (⭐)')
        axes[0, 1].set_yticks(range(len(category_means)))
        axes[0, 1].set_yticklabels(category_means.index)
        axes[0, 1].grid(True, alpha=0.3)

        # Agregar valores en las barras
        for i, bar in enumerate(bars):
            width = bar.get_width()
            axes[0, 1].text(width + 0.01, bar.get_y() + bar.get_height() / 2,
                            f'{width:.3f}', ha='left', va='center')

        # 3. Número de reviews por categoría
        category_counts = self.data['original_category'].value_counts()
        axes[1, 0].bar(range(len(category_counts)), category_counts.values,
                       color='lightcoral', alpha=0.7)
        axes[1, 0].set_title('Número de Reviews por Categoría')
        axes[1, 0].set_xlabel('Categoría')
        axes[1, 0].set_ylabel('Número de Reviews')
        axes[1, 0].set_xticks(range(len(category_counts)))
        axes[1, 0].set_xticklabels(category_counts.index, rotation=45)
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Porcentaje de excelencia por categoría
        excellence_pct = self.data.groupby('original_category').apply(
            lambda x: (x['overall'] >= 4.5).sum() / len(x) * 100
        ).sort_values(ascending=True)

        bars = axes[1, 1].barh(range(len(excellence_pct)), excellence_pct.values,
                               color='darkgreen', alpha=0.7)
        axes[1, 1].set_title('Porcentaje de Excelencia por Categoría (≥4.5⭐)')
        axes[1, 1].set_xlabel('Porcentaje (%)')
        axes[1, 1].set_yticks(range(len(excellence_pct)))
        axes[1, 1].set_yticklabels(excellence_pct.index)
        axes[1, 1].grid(True, alpha=0.3)

        # Agregar valores
        for i, bar in enumerate(bars):
            width = bar.get_width()
            axes[1, 1].text(width + 0.5, bar.get_y() + bar.get_height() / 2,
                            f'{width:.1f}%', ha='left', va='center')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')

        return fig

    def plot_interactive_dashboard(self) -> go.Figure:
        """
        Crea dashboard interactivo con Plotly

        Returns:
            Figura interactiva de Plotly
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        # Crear subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                'Distribución de Ratings',
                'Ratings por Categoría',
                'Evolución Temporal',
                'Top Productos'
            ],
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )

        # 1. Histograma de ratings
        fig.add_trace(
            go.Histogram(
                x=self.data['overall'],
                name='Distribución',
                marker_color='skyblue',
                opacity=0.7
            ),
            row=1, col=1
        )

        # 2. Box plot por categorías (si disponible)
        if 'original_category' in self.data.columns:
            for i, category in enumerate(self.data['original_category'].unique()):
                category_data = self.data[self.data['original_category'] == category]['overall']
                fig.add_trace(
                    go.Box(
                        y=category_data,
                        name=category,
                        marker_color=self.plotly_colors[i % len(self.plotly_colors)]
                    ),
                    row=1, col=2
                )

        # 3. Evolución temporal (si disponible)
        if 'unixReviewTime' in self.data.columns:
            self.data['review_date'] = pd.to_datetime(self.data['unixReviewTime'], unit='s', errors='coerce')
            valid_dates = self.data.dropna(subset=['review_date'])

            if not valid_dates.empty:
                valid_dates['year'] = valid_dates['review_date'].dt.year
                yearly_stats = valid_dates.groupby('year')['overall'].mean()

                fig.add_trace(
                    go.Scatter(
                        x=yearly_stats.index,
                        y=yearly_stats.values,
                        mode='lines+markers',
                        name='Rating Promedio',
                        line=dict(color='red', width=3),
                        marker=dict(size=8)
                    ),
                    row=2, col=1
                )

        # 4. Top productos (si hay múltiples reviews por producto)
        product_stats = self.data.groupby('asin')['overall'].agg(['count', 'mean'])
        top_products = product_stats[product_stats['count'] >= 2].sort_values('mean', ascending=False).head(10)

        if not top_products.empty:
            fig.add_trace(
                go.Bar(
                    x=list(range(len(top_products))),
                    y=top_products['mean'],
                    name='Top Productos',
                    marker_color='lightgreen',
                    text=[f'{rating:.2f}⭐' for rating in top_products['mean']],
                    textposition='auto'
                ),
                row=2, col=2
            )

        # Actualizar layout
        fig.update_layout(
            title_text="Dashboard Interactivo - Amazon Reviews Analysis",
            showlegend=False,
            template=self.plotly_template,
            height=800
        )

        return fig

    def plot_group_comparison(self, save_path: Optional[str] = None) -> plt.Figure:
        """
        Compara grupos Entertainment vs Home

        Args:
            save_path: Ruta para guardar el gráfico

        Returns:
            Figura de matplotlib
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        if 'category_group' not in self.data.columns:
            raise ValueError("No hay información de grupos")

        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Comparación Entertainment vs Home', fontsize=16, fontweight='bold')

        # 1. Distribución comparativa
        entertainment_data = self.data[self.data['category_group'] == 'Entertainment']['overall']
        home_data = self.data[self.data['category_group'] == 'Home']['overall']

        axes[0].hist([entertainment_data, home_data], bins=15, alpha=0.7,
                     label=['Entertainment', 'Home'], color=['lightcoral', 'lightgreen'])
        axes[0].set_title('Distribución de Ratings')
        axes[0].set_xlabel('Rating (⭐)')
        axes[0].set_ylabel('Frecuencia')
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)

        # 2. Box plot comparativo
        sns.boxplot(data=self.data, x='category_group', y='overall', ax=axes[1])
        axes[1].set_title('Comparación de Distribuciones')
        axes[1].set_xlabel('Grupo')
        axes[1].set_ylabel('Rating (⭐)')
        axes[1].grid(True, alpha=0.3)

        # 3. Estadísticas comparativas
        group_stats = self.data.groupby('category_group')['overall'].agg(['mean', 'std', 'count'])

        x_pos = range(len(group_stats))
        means = group_stats['mean']
        stds = group_stats['std']

        bars = axes[2].bar(x_pos, means, yerr=stds, capsize=5,
                           color=['lightcoral', 'lightgreen'], alpha=0.7)
        axes[2].set_title('Comparación de Promedios')
        axes[2].set_xlabel('Grupo')
        axes[2].set_ylabel('Rating Promedio (⭐)')
        axes[2].set_xticks(x_pos)
        axes[2].set_xticklabels(group_stats.index)
        axes[2].grid(True, alpha=0.3)

        # Agregar valores
        for i, bar in enumerate(bars):
            height = bar.get_height()
            axes[2].text(bar.get_x() + bar.get_width() / 2., height + 0.05,
                         f'{height:.3f}', ha='center', va='bottom')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')

        return fig

    def save_all_plots(self, output_dir: str) -> Dict[str, str]:
        """
        Genera y guarda todos los gráficos

        Args:
            output_dir: Directorio de salida

        Returns:
            Diccionario con rutas de archivos guardados
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        saved_files = {}

        try:
            # Distribución de ratings
            file_path = output_path / "rating_distribution.png"
            self.plot_rating_distribution(str(file_path))
            saved_files['rating_distribution'] = str(file_path)
            plt.close()

            # Niveles de satisfacción
            file_path = output_path / "satisfaction_levels.png"
            self.plot_satisfaction_levels(str(file_path))
            saved_files['satisfaction_levels'] = str(file_path)
            plt.close()

            # Comparación por categorías
            if 'original_category' in self.data.columns:
                file_path = output_path / "category_comparison.png"
                self.plot_category_comparison(str(file_path))
                saved_files['category_comparison'] = str(file_path)
                plt.close()

            # Comparación de grupos
            if 'category_group' in self.data.columns:
                file_path = output_path / "group_comparison.png"
                self.plot_group_comparison(str(file_path))
                saved_files['group_comparison'] = str(file_path)
                plt.close()

            # Dashboard interactivo
            file_path = output_path / "interactive_dashboard.html"
            interactive_fig = self.plot_interactive_dashboard()
            interactive_fig.write_html(str(file_path))
            saved_files['interactive_dashboard'] = str(file_path)

            self.logger.info(f"Guardados {len(saved_files)} gráficos en {output_dir}")

        except Exception as e:
            self.logger.error(f"Error guardando gráficos: {e}")
            raise

        return saved_files


# Función de utilidad para usar el visualizador directamente
def create_visualization_report(data: pd.DataFrame, output_dir: str) -> Dict[str, str]:
    """
    Función de utilidad para crear reporte completo de visualizaciones

    Args:
        data: DataFrame con los datos
        output_dir: Directorio de salida

    Returns:
        Diccionario con archivos generados
    """
    visualizer = DataVisualizer(data)
    return visualizer.save_all_plots(output_dir)


if __name__ == "__main__":
    print("📈 MÓDULO DE VISUALIZACIÓN")
    print("=" * 40)
    print("📊 DataVisualizer - Clase principal de visualización")
    print("🎨 Funciones disponibles:")
    print("   • plot_rating_distribution()")
    print("   • plot_satisfaction_levels()")
    print("   • plot_category_comparison()")
    print("   • plot_interactive_dashboard()")
    print("   • save_all_plots()")
    print()
    print("💡 Para usar:")
    print("   from src.analysis.visualizer import DataVisualizer")
    print("   visualizer = DataVisualizer(data)")
    print("   visualizer.plot_rating_distribution()")