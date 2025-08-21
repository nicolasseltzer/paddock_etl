# ETL Normalización Espacial de Paddocks

## Descripción General

Este ETL (Extract, Transform, Load) resuelve el problema de IDs de paddocks que cambian año a año, utilizando geometrías espaciales para mapear paddocks históricos a paddocks de referencia. El objetivo es crear series temporales consistentes para análisis de datos de granjas.

## Problema que Resuelve

- **Problema**: Los paddocks cambian de ID entre años, dificultando el análisis temporal
- **Solución**: Usar geometrías espaciales para identificar paddocks equivalentes
- **Resultado**: Dataset normalizado con IDs consistentes para análisis de series temporales

## Arquitectura del Proyecto

```
paddock_etl/
├── config.py              # Configuración de BD y reglas de agregación
├── database.py            # Conexiones y consultas SQL
├── spatial_matching.py    # Lógica de matching espacial
├── data_processing.py     # Agregación y transformación de datos
├── utils.py               # Funciones auxiliares
├── main.py                # Orquestador principal
├── README.md              # Esta documentación
└── requirements.txt       # Dependencias Python
```

## Flujo de Procesamiento

### 1. Extracción (Extract)
- Conecta a PostgreSQL/PostGIS
- Extrae datos de una granja específica (`farm_id`)
- Combina tabla de datos con tabla de geometrías de paddocks

### 2. Transformación (Transform)

#### 2.1 Matching Espacial
- Determina año de referencia (más reciente)
- Crea conjunto de paddocks de referencia
- Para cada paddock histórico:
  - Calcula superposición espacial con paddocks de referencia
  - Encuentra el mejor match (mayor superposición)
  - Crea mapeo: `paddock_histórico → paddock_referencia`

#### 2.2 Agregación de Datos
- Aplica mapeo a todos los datos
- Agrupa paddocks que mapean al mismo paddock de referencia
- Aplica reglas de agregación según tipo de métrica:
  - **Distributivas** (ej: cantidad de animales): suma
  - **Intensivas** (ej: carga animal/ha): promedio
  - **Categóricas** (ej: tipo de cultivo): primer valor o mayoría

### 3. Carga (Load)
- Guarda resultado en archivo CSV local
- Estructura final: una fila por paddock normalizado + año
- Namespaces como columnas separadas

## Módulos Detallados

### config.py
**Propósito**: Configuración centralizada

**Componentes**:
- `DatabaseConfig`: Credenciales y conexión a BD
- `ETLConfig`: Nombres de tablas y reglas de agregación por namespace

**Configuración Importante**:
```python
namespace_aggregation_rules = {
    "livestock": {
        "animal_count": "sum",        # Métrica distributiva
        "stocking_rate": "mean",      # Métrica intensiva  
        "animal_type": "first"        # Métrica categórica
    }
}
```

### database.py
**Propósito**: Manejo de base de datos

**Funciones Principales**:
- `get_farm_data_with_geometries()`: Extrae datos de una granja con geometrías
- `get_all_farm_ids()`: Obtiene lista de todas las granjas
- `test_connection()`: Valida conexión a BD

**Query Principal**:
```sql
SELECT d.year, d.namespace, d.data, p.geometry, ...
FROM data_table d
JOIN paddocks_table p ON d.paddock_id = p.id  
WHERE d.farm_id = %(farm_id)s
```

### spatial_matching.py
**Propósito**: Lógica de matching espacial

**Proceso**:
1. `determine_reference_year()`: Encuentra año más reciente
2. `create_reference_paddocks()`: Crea conjunto de referencia
3. `find_best_spatial_match()`: Calcula superposición espacial usando Shapely
4. `create_paddock_mapping()`: Genera mapeo completo

**Algoritmo de Matching**:
- Calcula `intersection.area / historical_paddock.area`
- Selecciona paddock de referencia con mayor superposición
- Requiere superposición mínima (configurable, default: 30%)

### data_processing.py
**Propósito**: Agregación y transformación de datos

**Funciones**:
- `combine_namespace_data()`: Combina datos de paddocks que mapean al mismo paddock de referencia
- `_apply_aggregation_rule()`: Aplica reglas específicas por tipo de métrica
- `normalize_farm_data()`: Proceso completo de normalización

**Reglas de Agregación**:
- `"sum"`: Para métricas distributivas (cantidad total)
- `"mean"`: Para métricas intensivas (densidad, ratios)
- `"first"`: Para categóricas (toma primer valor)
- `"majority"`: Para categóricas (valor más frecuente)
- `"divide_by_paddock_count"`: Para casos especiales

### utils.py
**Propósito**: Funciones auxiliares

**Funciones**:
- `setup_logging()`: Configura logging con archivos y consola
- `save_results()`: Guarda CSV con timestamp automático
- `validate_farm_data()`: Valida calidad de datos procesados

### main.py
**Propósito**: Orquestador principal

**Clase Principal**: `PaddockETL`

**Métodos**:
- `process_single_farm()`: Procesa una granja específica
- `process_all_farms()`: Procesa todas las granjas disponibles

## Instalación y Configuración

### 1. Dependencias
```bash
pip install pandas geopandas sqlalchemy psycopg2-binary
```

### 2. Configuración de Base de Datos
Editar `config.py`:
```python
db_config = DatabaseConfig(
    host="tu_host",
    database="tu_database", 
    username="tu_usuario",
    password="tu_password"
)

etl_config = ETLConfig(
    data_table="nombre_tabla_datos",
    paddocks_table="nombre_tabla_paddocks"
)
```

### 3. Configurar Reglas de Agregación
Personalizar `namespace_aggregation_rules` según tus namespaces específicos.

## Uso

### Procesar Una Granja
```python
from main import PaddockETL
from config import DatabaseConfig, ETLConfig

# Configurar
db_config = DatabaseConfig(...)
etl_config = ETLConfig(...)
etl = PaddockETL(db_config, etl_config)

# Procesar
farm_id = "bccb9e66-eda7-4620-840c-01b71810b86a"
output_path = etl.process_single_farm(farm_id)
```

### Procesar Todas las Granjas
```python
# Procesar todas
all_results = etl.process_all_farms(output_dir="./resultados")

# O lista específica
farm_list = ["granja1", "granja2", "granja3"]
results = etl.process_all_farms(farm_ids=farm_list)
```

## Estructura de Datos

### Input (Base de Datos)
**Tabla de Datos**:
```
id | farm_id | year | namespace | data (JSON) | paddock_id
```

**Tabla de Paddocks**:
```
id | geometry (PostGIS) | created_at | updated_at
```

### Output (CSV)
```
normalized_paddock_id | year | namespace_1 | namespace_2 | ... | namespace_N
abc123               | 2023 | {"data":...} | {"data":...} | ... | {"data":...}
abc123               | 2022 | {"data":...} | {"data":...} | ... | {"data":...}
```

## Casos de Uso Especiales

### Splits de Paddocks (1→N)
- **Problema**: Un paddock se divide en varios
- **Solución**: Todos mapean al paddock de referencia más grande
- **Agregación**: Datos se suman/promedian según reglas

### Merges de Paddocks (N→1)  
- **Problema**: Varios paddocks se unen en uno
- **Solución**: Múltiples paddocks históricos mapean a uno de referencia
- **Agregación**: Datos se combinan según reglas

### Paddocks Sin Match
- **Problema**: Superposición < umbral mínimo
- **Solución**: Mantiene ID original (self-mapping)
- **Log**: Warning para revisión manual

## Logging y Debugging

### Niveles de Log
- `INFO`: Progreso general y estadísticas
- `WARNING`: Problemas no críticos (ej: baja superposición)
- `ERROR`: Errores que impiden procesamiento
- `DEBUG`: Detalles de matching individual

### Archivos de Log
```bash
# Configurar en main.py
setup_logging("INFO", "etl.log")
```

### Estadísticas de Validación
Cada ejecución genera:
- Total de registros procesados
- Años cubiertos
- Número de paddocks únicos
- Porcentajes de valores nulos por columna
- Estadísticas de mapeo espacial

## Optimizaciones

### Performance
- **Consulta por granja**: Evita cargar toda la BD en memoria
- **Índices recomendados**: `farm_id`, `year`, `paddock_id` 
- **Geometrías**: Usar índices espaciales en PostGIS

### Paralelización Futura
La estructura permite fácil paralelización:
```python
# Pseudocódigo para múltiples procesos
from multiprocessing import Pool

def process_farm_wrapper(farm_id):
    return etl.process_single_farm(farm_id)

with Pool(4) as pool:
    results = pool.map(process_farm_wrapper, farm_ids)
```

## Troubleshooting

### Errores Comunes

**"No se encontraron datos para granja X"**
- Verificar que `farm_id` existe en la BD
- Revisar nombres de tablas en `config.py`

**"Error calculando intersección"**  
- Geometrías corruptas o inválidas
- Usar `ST_MakeValid()` en PostGIS

**"Baja superposición"**
- Ajustar `min_overlap_threshold` en config
- Revisar calidad de geometrías
- Posible necesidad de mapeo manual

### Validación de Resultados
```python
# Verificar que años están completos
df_result = pd.read_csv("output.csv")
print(df_result.groupby('year').size())

# Verificar distribución de paddocks  
print(df_result['normalized_paddock_id'].value_counts())
```

## Próximos Pasos

1. **ETL 2**: Aplanar JSONs de namespaces en columnas individuales
2. **Automatización**: Integrar con Apache Airflow o similar
3. **Testing**: Agregar tests unitarios para cada módulo
4. **UI**: Dashboard para monitorear calidad de mapeos

## Licencia

[Especificar licencia según proyecto]

## Contacto

[Información de contacto para soporte]