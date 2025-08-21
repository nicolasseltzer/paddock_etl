# config.py
import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "your_database"
    username: str = "your_username"
    password: str = "your_password"
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class ETLConfig:
    # Nombres de tablas (ajustar según tu BD)
    data_table: str = "data_table"
    paddocks_table: str = "paddocks_table"
    
    # Configuraciones del matching espacial
    min_overlap_threshold: float = 0.3
    
    # Configuraciones de agregación por namespace
    namespace_aggregation_rules: Dict[str, Dict[str, str]] = None
    
    def __post_init__(self):
        if self.namespace_aggregation_rules is None:
            # Reglas por defecto - personalizar según tus namespaces
            self.namespace_aggregation_rules = {
                "livestock": {
                    "animal_count": "sum",  # distributiva
                    "stocking_rate": "mean",  # intensiva
                    "animal_type": "first"  # categórica
                },
                "production": {
                    "total_yield": "sum",
                    "yield_per_ha": "mean",
                    "crop_type": "first"
                }
                # Agregar más namespaces según necesites
            }