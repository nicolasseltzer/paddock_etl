# spatial_matching.py
import geopandas as gpd
import pandas as pd
from typing import Dict, Tuple
import logging
from config import ETLConfig

logger = logging.getLogger(__name__)

class SpatialMatcher:
    def __init__(self, etl_config: ETLConfig):
        self.config = etl_config
        self.reference_year = None
        self.reference_paddocks = None
        self.paddock_mapping = {}
    
    def determine_reference_year(self, gdf: gpd.GeoDataFrame) -> int:
        """
        Determina el año de referencia (el más reciente)
        """
        self.reference_year = gdf['year'].max()
        logger.info(f"Año de referencia establecido: {self.reference_year}")
        return self.reference_year
    
    def create_reference_paddocks(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Crea el conjunto de paddocks de referencia del año más reciente
        """
        ref_data = gdf[gdf['year'] == self.reference_year].copy()
        
        # Agrupar por paddock_id para obtener la geometría más reciente
        self.reference_paddocks = (ref_data
                                 .groupby('data_paddock_id')
                                 .first()
                                 .reset_index())
        
        self.reference_paddocks = gpd.GeoDataFrame(
            self.reference_paddocks, 
            geometry='geometry'
        )
        
        logger.info(f"Creados {len(self.reference_paddocks)} paddocks de referencia")
        return self.reference_paddocks
    
    def find_best_spatial_match(self, historical_geom, historical_paddock_id: str) -> Tuple[str, float]:
        """
        Encuentra el paddock de referencia que mejor coincide espacialmente
        
        Returns:
            (best_match_id, overlap_ratio)
        """
        max_overlap = 0
        best_match = None
        
        for idx, ref_paddock in self.reference_paddocks.iterrows():
            ref_geom = ref_paddock['geometry']
            
            try:
                # Calcular intersección
                intersection = historical_geom.intersection(ref_geom)
                
                if intersection.is_empty:
                    continue
                
                overlap_area = intersection.area
                historical_area = historical_geom.area
                
                if historical_area == 0:
                    continue
                
                overlap_ratio = overlap_area / historical_area
                
                if overlap_ratio > max_overlap:
                    max_overlap = overlap_ratio
                    best_match = ref_paddock['data_paddock_id']
                    
            except Exception as e:
                logger.warning(f"Error calculando intersección para paddock {historical_paddock_id}: {e}")
                continue
        
        return best_match, max_overlap
    
    def create_paddock_mapping(self, gdf: gpd.GeoDataFrame) -> Dict[str, str]:
        """
        Crea el mapeo completo de paddock_id histórico -> paddock_id de referencia
        """
        logger.info("Iniciando mapeo espacial de paddocks...")
        
        # Obtener paddocks únicos de años históricos
        historical_paddocks = gdf[gdf['year'] != self.reference_year].copy()
        unique_historical = historical_paddocks.drop_duplicates('data_paddock_id')
        
        for idx, paddock in unique_historical.iterrows():
            paddock_id = paddock['data_paddock_id']
            best_match, overlap_ratio = self.find_best_spatial_match(
                paddock['geometry'], 
                paddock_id
            )
            
            if best_match and overlap_ratio >= self.config.min_overlap_threshold:
                self.paddock_mapping[paddock_id] = best_match
                logger.debug(f"Mapped {paddock_id} -> {best_match} (overlap: {overlap_ratio:.2%})")
            else:
                logger.warning(f"No hay match válido para paddock {paddock_id} (overlap: {overlap_ratio:.2%})")
                # Mantener ID original si no hay match suficiente
                self.paddock_mapping[paddock_id] = paddock_id
        
        # Los paddocks de referencia se mapean a sí mismos
        for ref_paddock_id in self.reference_paddocks['data_paddock_id']:
            self.paddock_mapping[ref_paddock_id] = ref_paddock_id
        
        logger.info(f"Mapeo completado: {len(self.paddock_mapping)} paddocks mapeados")
        return self.paddock_mapping
    
    def get_mapping_stats(self) -> Dict:
        """
        Retorna estadísticas del mapeo para debugging
        """
        total_mapped = len(self.paddock_mapping)
        self_mapped = sum(1 for k, v in self.paddock_mapping.items() if k == v)
        remapped = total_mapped - self_mapped
        
        return {
            "total_paddocks": total_mapped,
            "self_mapped": self_mapped,
            "remapped": remapped,
            "reference_year": self.reference_year,
            "reference_paddocks_count": len(self.reference_paddocks) if self.reference_paddocks is not None else 0
        }