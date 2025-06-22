"""
Módulo de exploración de datos
Análisis estadístico y exploración de patterns en reviews de Amazon
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime
from scipy import stats


class DataExplorer:
    """Clase para análisis exploratorio de datos de reviews"""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        """
        Inicializa el explorador de datos

        Args:
            data: DataFrame con los datos a analizar
        """
        self.data = data
        self.logger = logging.getLogger(__name__)
        self._analysis_cache = {}

    def load_data(self, data: pd.DataFrame) -> None:
        """
        Carga datos para análisis

        Args:
            data: DataFrame con los datos
        """
        self.data = data
        self._analysis_cache = {}  # Limpiar cache
        self.logger.info(f"Datos cargados: {len(data)} registros")

    def basic_statistics(self) -> Dict[str, Any]:
        """
        Calcula estadísticas descriptivas básicas

        Returns:
            Diccionario con estadísticas básicas
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        stats = {
            'total_reviews': len(self.data),
            'rating_stats': self.data['overall'].describe().to_dict(),
            'unique_products': self.data['asin'].nunique() if 'asin' in self.data.columns else 0,
            'unique_reviewers': self.data['reviewerID'].nunique() if 'reviewerID' in self.data.columns else 0,
        }

        # Agregar estadísticas por categoría si están disponibles
        if 'original_category' in self.data.columns:
            stats['categories'] = self.data['original_category'].nunique()
            stats['category_distribution'] = self.data['original_category'].value_counts().to_dict()

        return stats

    def satisfaction_analysis(self) -> Dict[str, Any]:
        """
        Analiza niveles de satisfacción

        Returns:
            Análisis de satisfacción por niveles
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        # Crear niveles de satisfacción
        self.data['satisfaction_level'] = pd.cut(
            self.data['overall'],
            bins=[0, 2.5, 3.5, 4.5, 5.1],
            labels=['Baja', 'Media', 'Buena', 'Excelente']
        )

        satisfaction_dist = self.data['satisfaction_level'].value_counts()
        satisfaction_pct = (satisfaction_dist / len(self.data) * 100).round(1)

        return {
            'distribution_counts': satisfaction_dist.to_dict(),
            'distribution_percentages': satisfaction_pct.to_dict(),
            'excellent_ratio': satisfaction_pct.get('Excelente', 0),
            'problematic_ratio': satisfaction_pct.get('Baja', 0)
        }

    def category_analysis(self) -> Dict[str, Any]:
        """
        Análisis por categorías de productos

        Returns:
            Estadísticas por categoría
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        if 'original_category' not in self.data.columns:
            return {'error': 'No hay información de categorías disponible'}

        category_stats = self.data.groupby('original_category')['overall'].agg([
            'count', 'mean', 'std', 'min', 'max', 'median'
        ]).round(3)

        # Calcular ratios de excelencia por categoría
        excellence_ratios = self.data.groupby('original_category').apply(
            lambda x: (x['overall'] >= 4.5).sum() / len(x) * 100
        ).round(1)

        # Ranking por rating promedio
        ranking = category_stats.sort_values('mean', ascending=False)

        return {
            'statistics': category_stats.to_dict('index'),
            'excellence_ratios': excellence_ratios.to_dict(),
            'ranking': ranking.index.tolist(),
            'best_category': ranking.index[0],
            'worst_category': ranking.index[-1],
            'rating_range': ranking['mean'].max() - ranking['mean'].min()
        }

    def group_comparison(self) -> Dict[str, Any]:
        """
        Comparación entre grupos de categorías (Entertainment vs Home)

        Returns:
            Análisis comparativo de grupos
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        if 'category_group' not in self.data.columns:
            return {'error': 'No hay información de grupos disponible'}

        group_stats = self.data.groupby('category_group')['overall'].agg([
            'count', 'mean', 'std', 'median'
        ]).round(3)

        # Test estadístico si hay dos grupos
        groups = self.data['category_group'].unique()
        statistical_test = None

        if len(groups) == 2:
            group1_data = self.data[self.data['category_group'] == groups[0]]['overall']
            group2_data = self.data[self.data['category_group'] == groups[1]]['overall']

            try:
                t_stat, p_value = stats.ttest_ind(group1_data, group2_data)
                statistical_test = {
                    'test': 't-test',
                    't_statistic': float(t_stat),
                    'p_value': float(p_value),
                    'significant': p_value < 0.05
                }
            except Exception as e:
                self.logger.warning(f"Error en test estadístico: {e}")

        return {
            'group_statistics': group_stats.to_dict('index'),
            'statistical_test': statistical_test,
            'best_group': group_stats['mean'].idxmax(),
            'difference': abs(group_stats['mean'].iloc[0] - group_stats['mean'].iloc[1]) if len(group_stats) >= 2 else 0
        }

    def product_analysis(self, min_reviews: int = 2) -> Dict[str, Any]:
        """
        Análisis de productos individuales

        Args:
            min_reviews: Número mínimo de reviews por producto

        Returns:
            Análisis de productos
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        product_stats = self.data.groupby('asin')['overall'].agg([
            'count', 'mean', 'std'
        ]).round(3)

        # Filtrar productos con suficientes reviews
        qualified_products = product_stats[product_stats['count'] >= min_reviews]

        if len(qualified_products) == 0:
            return {'error': f'No hay productos con {min_reviews}+ reviews'}

        # Identificar productos estrella y problemáticos
        star_products = qualified_products[qualified_products['mean'] >= 4.5]
        problematic_products = qualified_products[qualified_products['mean'] <= 2.5]

        return {
            'total_products': len(product_stats),
            'qualified_products': len(qualified_products),
            'star_products': {
                'count': len(star_products),
                'percentage': len(star_products) / len(qualified_products) * 100,
                'top_5': star_products.sort_values('mean', ascending=False).head().to_dict('index')
            },
            'problematic_products': {
                'count': len(problematic_products),
                'percentage': len(problematic_products) / len(qualified_products) * 100,
                'worst_5': problematic_products.sort_values('mean').head().to_dict('index')
            }
        }

    def reviewer_analysis(self) -> Dict[str, Any]:
        """
        Análisis de comportamiento de reviewers

        Returns:
            Análisis de reviewers
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        reviewer_stats = self.data.groupby('reviewerID').agg({
            'overall': ['count', 'mean', 'std'],
            'original_category': 'nunique' if 'original_category' in self.data.columns else lambda x: 1
        }).round(3)

        reviewer_stats.columns = ['review_count', 'avg_rating', 'rating_std', 'categories_reviewed']

        # Clasificar reviewers por actividad
        activity_levels = pd.cut(
            reviewer_stats['review_count'],
            bins=[0, 1, 3, 5, float('inf')],
            labels=['Ocasional', 'Moderado', 'Activo', 'Muy Activo']
        )

        reviewer_stats['activity_level'] = activity_levels

        activity_summary = reviewer_stats.groupby('activity_level').agg({
            'review_count': ['count', 'sum', 'mean'],
            'avg_rating': 'mean'
        }).round(3)

        return {
            'total_reviewers': len(reviewer_stats),
            'activity_distribution': reviewer_stats['activity_level'].value_counts().to_dict(),
            'activity_summary': activity_summary.to_dict(),
            'most_active': {
                'reviewer_id': reviewer_stats['review_count'].idxmax(),
                'review_count': reviewer_stats['review_count'].max()
            }
        }

    def temporal_analysis(self) -> Dict[str, Any]:
        """
        Análisis temporal de reviews

        Returns:
            Análisis de tendencias temporales
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        if 'unixReviewTime' not in self.data.columns:
            return {'error': 'No hay información temporal disponible'}

        # Convertir timestamps a fechas
        self.data['review_date'] = pd.to_datetime(
            self.data['unixReviewTime'], unit='s', errors='coerce'
        )

        valid_dates = self.data.dropna(subset=['review_date'])

        if len(valid_dates) == 0:
            return {'error': 'No hay fechas válidas'}

        # Análisis por año
        valid_dates['year'] = valid_dates['review_date'].dt.year
        yearly_stats = valid_dates.groupby('year')['overall'].agg([
            'count', 'mean'
        ]).round(3)

        # Filtrar años con suficientes datos
        yearly_stats = yearly_stats[yearly_stats['count'] >= 10]

        # Calcular tendencia
        trend_correlation = None
        if len(yearly_stats) >= 3:
            trend_correlation = yearly_stats.index.to_series().corr(yearly_stats['mean'])

        return {
            'date_range': {
                'start': valid_dates['review_date'].min().strftime('%Y-%m-%d'),
                'end': valid_dates['review_date'].max().strftime('%Y-%m-%d')
            },
            'yearly_statistics': yearly_stats.to_dict('index'),
            'trend_correlation': float(trend_correlation) if trend_correlation else None,
            'trend_direction': 'Ascendente' if trend_correlation and trend_correlation > 0.1
            else 'Descendente' if trend_correlation and trend_correlation < -0.1
            else 'Estable'
        }

    def content_analysis(self) -> Dict[str, Any]:
        """
        Análisis de contenido de reviews

        Returns:
            Análisis de longitud y calidad de contenido
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        # Calcular longitudes de contenido
        self.data['review_length'] = self.data['reviewText'].str.len().fillna(0)
        self.data['summary_length'] = self.data['summary'].str.len().fillna(0)
        self.data['total_content_length'] = self.data['review_length'] + self.data['summary_length']

        # Crear categorías de longitud
        self.data['content_category'] = pd.cut(
            self.data['total_content_length'],
            bins=[0, 50, 200, 500, float('inf')],
            labels=['Muy Corto', 'Corto', 'Medio', 'Largo']
        )

        # Análisis por categoría de contenido
        content_analysis = self.data.groupby('content_category')['overall'].agg([
            'count', 'mean', 'std'
        ]).round(3)

        # Correlación entre longitud y rating
        length_rating_corr = self.data['total_content_length'].corr(self.data['overall'])

        return {
            'content_statistics': {
                'avg_review_length': float(self.data['review_length'].mean()),
                'avg_summary_length': float(self.data['summary_length'].mean()),
                'avg_total_length': float(self.data['total_content_length'].mean())
            },
            'content_distribution': self.data['content_category'].value_counts().to_dict(),
            'content_quality_analysis': content_analysis.to_dict('index'),
            'length_rating_correlation': float(length_rating_corr)
        }

    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """
        Genera un reporte comprensivo de todos los análisis

        Returns:
            Reporte completo con todos los análisis
        """
        if self.data is None:
            raise ValueError("No hay datos cargados")

        report = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_records': len(self.data),
                'analysis_version': '1.0.0'
            },
            'basic_statistics': self.basic_statistics(),
            'satisfaction_analysis': self.satisfaction_analysis(),
            'content_analysis': self.content_analysis()
        }

        # Agregar análisis opcionales si los datos están disponibles
        try:
            report['category_analysis'] = self.category_analysis()
        except Exception as e:
            self.logger.warning(f"Category analysis failed: {e}")

        try:
            report['group_comparison'] = self.group_comparison()
        except Exception as e:
            self.logger.warning(f"Group comparison failed: {e}")

        try:
            report['product_analysis'] = self.product_analysis()
        except Exception as e:
            self.logger.warning(f"Product analysis failed: {e}")

        try:
            report['reviewer_analysis'] = self.reviewer_analysis()
        except Exception as e:
            self.logger.warning(f"Reviewer analysis failed: {e}")

        try:
            report['temporal_analysis'] = self.temporal_analysis()
        except Exception as e:
            self.logger.warning(f"Temporal analysis failed: {e}")

        return report