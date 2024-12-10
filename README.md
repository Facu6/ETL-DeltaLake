# Proyecto: CoinLore Data Pipeline

Este proyecto implementa un pipeline de datos completo para la extracción, procesamiento y almacenamiento de información de criptomonedas utilizando datos provenientes de la API de CoinLore. Emplea diversas herramientas para manejar datos en tiempo real e históricos y garantiza la seguridad de información confidencial.

---

## Tabla de Contenidos
- [Descripción](#descripción)
- [Características](#características)
- [Requisitos Previos](#requisitos-previos)
- [Detalles del Pipeline](#detalles-del-pipeline)
- [Instalación](#instalación)
- [Ejecución](#ejecución-del-script)


---

## Descripción

El objetivo del proyecto es construir un pipeline de datos robusto para manejar datos de criptomonedas. El flujo del pipeline abarca desde la extracción incremental y completa de datos hasta el procesamiento, generación de columnas ficticias y enmascaramiento de datos sensibles, para posteriormente almacenarlos en capas `Bronze`, `Silver` y `Gold` del Delta Lake, cómo así también en una capa `Confidential` para aquellos datos (ficticios) que requieran cierta sensibilidad para su tratamiento.

En lo que respecta a la extracción incremental, se trabaja con un archivo json (`metadata_ingestion.json`) el cual permite obtener la fecha y hora de la última descarga, basándose en esta medida de tiempo para reconocer a partir de que momento extraer datos nuevos.

---

## Características

- **Extracción incremental**: Captura solo los datos nuevos desde el último proceso.
- **Extracción completa**: Descarga toda la información disponible desde la API.
- **Procesamiento de datos**: Limpieza, transformación y normalización de los datos.
- **Manejo de información confidencial**: Generación de correos ficticios y enmascaramiento de datos sensibles.
- **Almacenamiento jerarquizado**: Uso de capas `Bronze`, `Silver` y `Gold` en Delta Lake para diferenciar datos crudos, procesados y agregados.
- **Compatibilidad con Delta Lake**: Integración con `pandas` y `pyarrow` para guardar datos.

---

## Requisitos Previos

- **Python 3.8 o superior**
- Delta Lake instalado en tu entorno.
- **Librerías requeridas** (incluidas en `requirements.txt`):
  - requests
  - pandas
  - numpy
  - pyarrow
  - deltalake
  - faker
  - json

---

## Detalles del Pipeline

### Extracción de datos:
- La función `aplicar_extraccion_incremental` permite extraer datos nuevos desde el último proceso.
- La función `aplicar_extraccion_full` descarga toda la información disponible desde la API.

### Procesamiento de datos:
- Se realizan limpiezas y normalizaciones, como reemplazo de valores faltantes y estandarización de tipos de datos.
- Se incluyen columnas ficticias para fines confidenciales y columnas agregadas (`diff_`, `cumsum_`).

### Almacenamiento:
- **Bronze**: Datos crudos extraídos directamente de la API.
- **Silver**: Datos procesados y normalizados.
- **Gold**: Datos listos para análisis, incluyendo cálculos agregados.

---

## Instalación

1. **Clona el repositorio**:
   Clona el proyecto desde GitHub utilizando el siguiente comando:
   ```bash
   git clone https://github.com/usuario/coinlore-data-pipeline.git
   cd coinlore-data-pipeline

2. **Instala las dependencias: Instala las librerías requeridas especificadas en el archivo requirements.txt:**
    ```bash
    pip install -r requirements.txt

3. **Verifica la conexión a Delta Lake: Asegúrate de que tu entorno tiene configurado Delta Lake para guardar y cargar datos.**

---

## Ejecución del Script
1. **Define los parámetros iniciales: Configura los parámetros para la API y las rutas de Delta Lake en el script principal:**
    ```bash
    base_url = 'https://api.coinlore.net/api/{}'
    endpoint_temporal = 'ticker'
    endpoint_estatico = 'exchanges'
    params_temporal = {
        'id': ",".join(['122', '95', '106'])
    }

2. **Ejecuta el script principal: Ejecuta el archivo Python que contiene el pipeline:**
    ```bash
    python main.py
