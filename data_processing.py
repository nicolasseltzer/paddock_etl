# data_processing.py
import pandas as pd
import json
from typing import Dict, Any, List
import logging
from config import ETLConfig

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, etl_config: ETLConfig):
        self.config = etl_config
    
    def combine_namespace_data(self, group: pd.DataFrame, namespace: str, paddock_count: int) -> Dict[str, Any]:
        """
        Combina datos de múltiples paddocks que se mapean al mismo paddock de referencia
        """
        if len(group) == 1:
            return group.iloc[0]['data']
        
        logger.debug(f"Agregando {len(group)} registros para namespace {namespace}")
        
        # Obtener reglas de agregación para este namespace
        aggregation_rules = self.config.namespace_aggregation_rules.get(namespace, {})
        
        combined_data = {}
        
        # Procesar cada métrica en los datos
        all_keys = set()
        for _, row in group.iterrows():
            if isinstance(row['data'], dict):
                all_keys.update(row['data'].keys())
        
        for key in all_keys:
            values = []
            for _, row in group.iterrows():
                if isinstance(row['data'], dict) and key in row['data']:
                    values.append(row['data'][key])
            
            if not values:
                combined_data[key] = None
                continue
            
            # Aplicar regla de agregación
            rule = aggregation_rules.get(key, "first")  # default: tomar primer valor
            combined_data[key] = self._apply_aggregation_rule(values, rule, paddock_count)
        
        return combined_data
    
    def _apply_aggregation_rule(self, values: List[Any], rule: str, paddock_count: int) -> Any:
        """
        Aplica regla de agregación específica a una lista de valores
        """
        # Filtrar valores None/null
        clean_values = [v for v in values if v is not None]
        
        if not clean_values:
            return None
        
        try:
            if rule == "sum":
                # Para métricas distributivas
                return sum(float(v) for v in clean_values if isinstance(v, (int, float)))
            
            elif rule == "mean":
                # Para métricas intensivas
                numeric_values = [float(v) for v in clean_values if isinstance(v, (int, float))]
                return sum(numeric_values) / len(numeric_values) if numeric_values else None
            
            elif rule == "divide_by_paddock_count":
                # Para métricas que deben dividirse por número de paddocks
                total = sum(float(v) for v in clean_values if isinstance(v, (int, float)))
                return total / paddock_count if paddock_count > 0 else total
            
            elif rule == "first":
                # Para métricas categóricas o cuando no sabemos qué hacer
                return clean_values[0]
            
            elif rule == "majority":
                # Para categóricas: tomar el valor más frecuente
                from collections import Counter
                return Counter(clean_values).most_common(1)[0][0]
            
            else:
                logger.warning(f"Regla de agregación desconocida: {rule}. Usando 'first'")
                return clean_values[0]
                
        except Exception as e:
            logger.warning(f"Error aplicando regla {rule} a valores {values}: {e}")
            return clean_values[0] if clean_values else None
    
    def normalize_farm_data(self, gdf: pd.DataFrame, paddock_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Aplica el mapeo y normaliza los datos de una granja
        """
        logger.info("Aplicando normalización de datos...")
        
        # Aplicar mapeo de paddock_ids
        gdf['normalized_paddock_id'] = gdf['data_paddock_id'].map(paddock_mapping)
        
        # Validar que todos los paddocks fueron mapeados
        unmapped = gdf[gdf['normalized_paddock_id'].isna()]
        if len(unmapped) > 0:
            logger.warning(f"Encontrados {len(unmapped)} registros sin mapear")
        
        # Agrupar por paddock normalizado, año y namespace
        result_data = []
        
        grouped = gdf.groupby(['normalized_paddock_id', 'year', 'namespace'])
        
        for (norm_paddock_id, year, namespace), group in grouped:
            if pd.isna(norm_paddock_id):
                continue
                
            original_paddock_count = len(group['data_paddock_id'].unique())
            combined_data = self.combine_namespace_data(group, namespace, original_paddock_count)
            
            result_data.append({
                'normalized_paddock_id': norm_paddock_id,
                'year': year,
                'namespace': namespace,
                'data': json.dumps(combined_data) if combined_data else None,
                'original_paddock_count': original_paddock_count
            })
        
        result_df = pd.DataFrame(result_data)
        
        if result_df.empty:
            logger.warning("No se generaron datos normalizados")
            return pd.DataFrame()
        
        # Pivot para tener namespaces como columnas
        final_df = result_df.pivot_table(
            index=['normalized_paddock_id', 'year'],
            columns='namespace',
            values='data',
            aggfunc='first'
        ).reset_index()
        
        # Limpiar nombres de columnas
        final_df.columns.name = None
        
        logger.info(f"Normalización completada: {len(final_df)} registros finales")
        return final_df