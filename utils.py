# utils.py
import pandas as pd
import os
import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def setup_logging(log_level: str = "INFO", log_file: str = None):
    """
    Configura el sistema de logging
    """
    level = getattr(logging, log_level.upper())
    
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def save_results(df: pd.DataFrame, output_path: str, farm_id: str = None):
    """
    Guarda los resultados en archivo local con metadata
    """
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Agregar timestamp al nombre si no está especificado
    if farm_id:
        base_name = f"paddocks_normalized_{farm_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        output_path = os.path.join(os.path.dirname(output_path), f"{base_name}.csv")
    
    logger.info(f"Guardando {len(df)} registros en: {output_path}")
    
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Resultados guardados exitosamente")
        return output_path
    except Exception as e:
        logger.error(f"Error guardando resultados: {e}")
        raise

def validate_farm_data(df: pd.DataFrame, farm_id: str) -> Dict[str, Any]:
    """
    Valida la calidad de los datos procesados
    """
    validation_results = {
        "farm_id": farm_id,
        "total_records": len(df),
        "years_covered": sorted(df['year'].unique().tolist()) if 'year' in df.columns else [],
        "paddocks_count": df['normalized_paddock_id'].nunique() if 'normalized_paddock_id' in df.columns else 0,
        "namespaces": [col for col in df.columns if col not in ['normalized_paddock_id', 'year']],
        "null_percentages": {}
    }
    
    # Calcular porcentajes de valores nulos por columna
    for col in df.columns:
        null_pct = (df[col].isna().sum() / len(df)) * 100
        validation_results["null_percentages"][col] = round(null_pct, 2)
    
    return validation_results