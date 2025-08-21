#main.py
import logging
from typing import List, Optional
from config import DatabaseConfig, ETLConfig
from database import DatabaseManager
from spatial_matching import SpatialMatcher
from data_processing import DataProcessor
from utils import setup_logging, save_results, validate_farm_data

logger = logging.getLogger(__name__)

class PaddockETL:
    def __init__(self, db_config: DatabaseConfig, etl_config: ETLConfig):
        self.db_config = db_config
        self.etl_config = etl_config
        self.db_manager = DatabaseManager(db_config)
        self.spatial_matcher = SpatialMatcher(etl_config)
        self.data_processor = DataProcessor(etl_config)
    
    def process_single_farm(self, farm_id: str, output_dir: str = "./output") -> str:
        """
        Procesa una sola granja y retorna el path del archivo generado
        """
        logger.info(f"Iniciando procesamiento de granja: {farm_id}")
        
        try:
            # 1. Extraer datos
            gdf = self.db_manager.get_farm_data_with_geometries(farm_id, self.etl_config)
            
            if gdf.empty:
                logger.warning(f"No se encontraron datos para granja {farm_id}")
                return None
            
            # 2. Matching espacial
            self.spatial_matcher.determine_reference_year(gdf)
            self.spatial_matcher.create_reference_paddocks(gdf)
            paddock_mapping = self.spatial_matcher.create_paddock_mapping(gdf)
            
            # Log estadísticas del mapeo
            stats = self.spatial_matcher.get_mapping_stats()
            logger.info(f"Estadísticas del mapeo: {stats}")
            
            # 3. Procesar y normalizar datos
            normalized_df = self.data_processor.normalize_farm_data(gdf, paddock_mapping)
            
            if normalized_df.empty:
                logger.warning(f"No se generaron datos normalizados para granja {farm_id}")
                return None
            
            # 4. Validar resultados
            validation = validate_farm_data(normalized_df, farm_id)
            logger.info(f"Validación: {validation}")
            
            # 5. Guardar resultados
            output_path = save_results(normalized_df, f"{output_dir}/output.csv", farm_id)
            
            logger.info(f"Procesamiento de granja {farm_id} completado exitosamente")
            return output_path
            
        except Exception as e:
            logger.error(f"Error procesando granja {farm_id}: {e}")
            raise
    
    def process_all_farms(self, output_dir: str = "./output", farm_ids: Optional[List[str]] = None) -> List[str]:
        """
        Procesa todas las granjas o una lista específica
        """
        if farm_ids is None:
            farm_ids = self.db_manager.get_all_farm_ids(self.etl_config)
        
        logger.info(f"Iniciando procesamiento de {len(farm_ids)} granjas")
        
        results = []
        for i, farm_id in enumerate(farm_ids, 1):
            logger.info(f"Procesando granja {i}/{len(farm_ids)}: {farm_id}")
            
            try:
                output_path = self.process_single_farm(farm_id, output_dir)
                if output_path:
                    results.append(output_path)
            except Exception as e:
                logger.error(f"Fallo en granja {farm_id}: {e}")
                # Continuar con las siguientes granjas
                continue
        
        logger.info(f"Procesamiento completado. {len(results)} granjas procesadas exitosamente")
        return results

# Ejemplo de uso
if __name__ == "__main__":
    # Configurar logging
    setup_logging("INFO", "etl.log")
    
    # Configuración
    db_config = DatabaseConfig(
        host="localhost",
        database="your_database",
        username="your_username",
        password="your_password"
    )
    
    etl_config = ETLConfig(
        data_table="your_data_table",
        paddocks_table="your_paddocks_table"
    )
    
    # Crear ETL
    etl = PaddockETL(db_config, etl_config)
    
    # Procesar una granja específica
    farm_id = "bccb9e66-eda7-4620-840c-01b71810b86a"
    result_path = etl.process_single_farm(farm_id)
    
    if result_path:
        print(f"Resultados guardados en: {result_path}")
    
    # O procesar todas las granjas
    # all_results = etl.process_all_farms()