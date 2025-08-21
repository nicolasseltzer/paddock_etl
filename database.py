# database.py
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import logging
from config import DatabaseConfig, ETLConfig

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_config: DatabaseConfig):
        self.config = db_config
        self.engine = create_engine(self.config.connection_string)
    
    def get_farm_data_with_geometries(self, farm_id: str, etl_config: ETLConfig) -> gpd.GeoDataFrame:
        """
        Extrae datos de una granja específica con sus geometrías
        """
        query = f"""
        SELECT 
            d.id,
            d.year,
            d.namespace,
            d.data,
            d.paddock_id as data_paddock_id,
            p.id as geometry_paddock_id,
            p.geometry,
            p.created_at,
            p.updated_at
        FROM {etl_config.data_table} d
        JOIN {etl_config.paddocks_table} p ON d.paddock_id = p.id
        WHERE d.farm_id = %(farm_id)s
        ORDER BY d.year DESC, p.updated_at DESC
        """
        
        logger.info(f"Extrayendo datos para granja: {farm_id}")
        
        try:
            gdf = gpd.read_postgis(
                query, 
                self.engine, 
                params={"farm_id": farm_id}, 
                geom_col='geometry'
            )
            logger.info(f"Extraídos {len(gdf)} registros para granja {farm_id}")
            return gdf
            
        except Exception as e:
            logger.error(f"Error extrayendo datos para granja {farm_id}: {e}")
            raise
    
    def get_all_farm_ids(self, etl_config: ETLConfig) -> List[str]:
        """
        Obtiene todos los farm_ids disponibles
        """
        query = f"SELECT DISTINCT farm_id FROM {etl_config.data_table}"
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                farm_ids = [row[0] for row in result]
            
            logger.info(f"Encontrados {len(farm_ids)} farm_ids")
            return farm_ids
            
        except Exception as e:
            logger.error(f"Error obteniendo farm_ids: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Prueba la conexión a la base de datos
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Conexión a BD exitosa")
            return True
        except Exception as e:
            logger.error(f"Error de conexión: {e}")
            return False