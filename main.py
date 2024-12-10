import requests
import pandas as pd 
import numpy as np
from datetime import datetime
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import json
import os 
from faker import Faker




# Archivo json en el cual se manejará la fecha y hora de última extracción para aplicar extracción incremental
json_file_path = 'metadata_ingestion.json'


# Función para obtener el último valor registrado en el archivo "json_file_path"
def obtener_ultimo_valor():
    '''Esta función abre un archivo JSON, extrae y devuelve el valor asociado con la clave "table_name".
    
    - Parámetros: 
        * No recibe parámetros.
    - Retorna valor asociado a clave'''
    
    try:
            # Abrimos el archivo JSON donde guardamos la última fecha extraída
            with open(json_file_path, 'r') as json_file_read:
                # Convierte el contenido Json a un objeto Python (diccionario)
                data_json = json.load(json_file_read)
            
            # Retorna el valor asociado con la clave "table_name" del diccionario
            return data_json['table_name']

    # Capturamos cualquier excepción que ocurra en el bloque try
    except Exception as e:
       print(f'Error: {e}')
       raise 

# Función para obtener el nuevo valor de la extracción incremental
def obtener_nuevo_valor(dato_metadata):
    '''Esta función extrae y formatea la fecha del encabezado 'Date' de la respuesta de una solicitud HTTP.
    
    - Parámetros:
        * dato_metadata: Un objeto de respuesta HTTP que contiene los encabezados de la solicitud.
    - Retorna nuevo_valor'''
    
    # Obtiene el valor del encabezado 'Date' de la respuesta HTTP
    nuevo_valor = dato_metadata.headers.get('Date')
    # Convierte la cadena de fecha obtenida en un objeto datetime
    nuevo_valor = datetime.strptime(nuevo_valor, '%a, %d %b %Y %H:%M:%S %Z')
    # Formatea el objeto datetime a una cadena con el formato "YYYY-MM-DD HH:MM:SS"
    nuevo_valor = nuevo_valor.strftime('%Y-%m-%d %H:%M:%S')
    
    # Devuelve la fecha formateada
    return nuevo_valor

# Función para actualizar el archivo "json_file_path" con el ultimo valor de la extracción incremental
def actualizar_utlimo_valor(nuevo_valor):
    '''Esta función actualiza el valor de la clave "last_value" en un archivo JSON con el nuevo valor proporcionado.
    
    - Parámetros:
        * nuevo_valor: El valor que se desea actualizar en el archivo JSON.
    - Retorna: None
    '''      
    
    # Abre el archivo "json_file_path" en modo lectura y escritura
    with open(json_file_path, 'r+') as file:
        # Carga el contenido del archivo JSON en un diccionario de Python
        data_json = json.load(file)
        # Actualiza el valor de 'last_value' dentro de 'table_name'
        data_json['table_name']['last_value'] = nuevo_valor 
        # Vuelve al inicio del archivo para sobrescribirlo  
        file.seek(0)
        # Escribe los datos actualizados en el archivo JSON con indentación
        json.dump(data_json, file, indent=4)

# Función para extraer datos de manera incremental   
def extraer_datos_incrementales(url, endpoint, params=None, headers=None):
    '''Esta función realiza una solicitud HTTP GET a una API para extraer datos incrementales.
    
    - Parámetros:
        * url: URL base de la API donde se hará la solicitud.
        * endpoint: El endpoint específico de la API que se debe incluir en la solicitud.
        * params: Parámetros adicionales para la solicitud (opcional).
        * headers: Encabezados HTTP que se incluyen en la solicitud (opcional).
    - Retorna:
        * r: La respuesta completa de la solicitud (objeto `requests.Response`).
        * r_json: Los datos extraídos de la API en formato JSON.
    '''
    try:
        
        # Formateamos la URL con el endpoint correspondiente para obtener la URL final
        url_final = url.format(endpoint)
        # Realizamos la solicitud GET a la API, pasando los parámetros si los hay
        r = requests.get(url_final, params=params, headers=headers)
        # Convertimos la respuesta JSON en un diccionario de Python
        r_json = r.json()
    
        # Devolvemos tanto la respuesta completa (r) como los datos extraídos en formato JSON (r_json)
        return r, r_json
    
    except Exception as e:
        print(f'Error en la extracción incremental de datos: {e}')
        return None, None

# Función para construir un dataframe        
def construir_dataframe(json_data):
    
    """
    Esta función convierte un objeto JSON (json_data) en un DataFrame de pandas utilizando la función 
    json_normalize de pandas.
    
    Parámetros:
    - json_data: Un objeto JSON (o un diccionario de Python) que contiene los datos que se desean convertir a un DataFrame.
    
    Retorna:
    - dataframe: Un DataFrame de pandas generado a partir del objeto JSON.
    """
    
    try:
        # Convertir el objeto JSON a un DataFrame utilizando json_normalize
        dataframe = pd.json_normalize(json_data)
        return dataframe

    except Exception as e:
        # Si ocurre un error, se imprime el mensaje de error
        print(f'Error en el intento de convertir los datos json a dataframe: {e}')

# Función para ejecutar la extracción incremental y todos sus componentes        
def aplicar_extraccion_incremental(url, endpoint, params=None, headers=None):       
        
    '''Esta función se encarga de realizar la extracción incremental de datos desde una API, procesarlos y 
    actualizar la información correspondiente en un archivo de metadata.

    Parámetros:
    - url: La URL base de la API desde donde se extraen los datos.
    - endpoint: El endpoint específico de la API que se consulta para obtener los datos.
    - params: (Opcional) Parámetros adicionales que se pueden pasar para la consulta.
    - headers: (Opcional) Encabezados HTTP adicionales que se pueden pasar para la consulta.

    Retorna:
    - dataframe'''
    try:    
        # Ejecuta la función "extraer_datos_incrementales" devolviendo "r" (response) y "datos" (en formato json)
        r, datos = extraer_datos_incrementales(url, endpoint, params=params, headers=headers)
        # Con los datos extraídos se construye un dataframe ---> Aplicando la función "construir_dataframe()"
        dataframe = construir_dataframe(datos)
        # Con "r" (response) se obtiene el último valor ---> Aplicando la función "obtener_nuevo_valor()"
        nuevo_valor = obtener_nuevo_valor(r)
        # Con "nuevo_valor" se actualiza el valor en el archivo "metadata_ingestion.json" ---> Aplicando la función "actualizar_ultimo_valor()"
        actualizar_utlimo_valor(nuevo_valor)
        
        print('La data incremental se extrajo con éxito.')
        # Retorna un Dataframe
        return dataframe

    except Exception as e:
        print(f'Error extrayendo la data incremental: {e}')

# Función para ejecutar la extracción full   
def aplicar_extraccion_full(url, endpoint, params=None, headers=None):
    
    """
    Esta función realiza la extracción completa de datos desde una API, procesando la respuesta y devolviendo 
    un DataFrame con los datos extraídos.

    Parámetros:
    - url: La URL base de la API con un marcador de posición para el endpoint.
    - endpoint: El endpoint específico de la API que se consulta.
    - params: (Opcional) Parámetros adicionales que se pueden pasar para la consulta.
    - headers: (Opcional) Encabezados HTTP adicionales que se pueden pasar para la consulta.

    Retorna:
    - dataframe_full: Un DataFrame de pandas con los datos extraídos de la API y procesados."""
    
    try:
        # Formatea la url con el endpoint pasados 
        url_final = url.format(endpoint)
        # Realiza la petición "get" a la API
        r = requests.get(url_final, params=params, headers=headers)
        # Convierte los datos extraídos a Json
        data_full = r.json()
        # Los datos son convertidos a Dataframe
        dataframe_full = construir_dataframe(data_full)
        
        print('La data full fue extraída con éxito.')
        # Retorna un Dataframe 
        return dataframe_full
    
    except Exception as e:
        print(f'Error extrayendo la data full: {e}')

# Función para procesar dataframe full con "melt" 
def procesamiento_melt_datos_full(dataframe):
    """
    Esta función procesa un DataFrame de criptomonedas utilizando un proceso de 'melt' y pivot para reorganizar
    los datos en un formato más manejable, devolviendo un DataFrame con las columnas específicas.

    Parámetros:
    - dataframe: El DataFrame con los datos de las criptomonedas a procesar.

    Retorna:
    - normalize_df: Un DataFrame reorganizado con las columnas específicas ['id', 'name', 'name_id', 
      'volume_usd', 'active_pairs', 'url', 'country']. """
    
    try:
        # Verifica si el DataFrame tiene menos de 17 columnas
        if len(dataframe.columns) < 17:
            print('El dataframe tiene menos de 17 columnas.')
        else:
            # Extrae los identificadores únicos a partir del nombre de las columnas
            unique_ids = set(col.split('.')[0] for col in dataframe.columns)
            
            # Realiza el 'melt' del DataFrame para convertir las columnas en filas
            melted_df = pd.melt(dataframe, var_name = 'variable', value_name = 'value')
            
            # Extrae 'crypto_id' y 'variable' de la columna 'variable' usando expresiones regulares
            melted_df[['crypto_id', 'variable']] = melted_df['variable'].str.extract(r'(\d+)\.(.*)')
            
            # Pivot de los datos con 'crypto_id' como índice y 'variable' como columnas
            normalize_df = melted_df.pivot_table(index = 'crypto_id', columns = 'variable', values = 'value', aggfunc = 'first').reset_index()
                
            # Elimina el nombre de las columnas en el DataFrame resultante
            normalize_df.columns.name = None
            
            # Selecciona las columnas de interés
            normalize_df = normalize_df[['id', 'name', 'name_id', 'volume_usd', 'active_pairs', 'url', 'country']]

            # Devuelve el DataFrame procesado
            return normalize_df

    except Exception as e:
        print(f'Error: {e}')

# Función para procesar la data full 
def procesamiento_datos_full(dataframe, diccionario_paises):
    """
    Esta función realiza varias transformaciones y limpieza de datos en un DataFrame de criptomonedas.
    Reemplaza valores vacíos por NaN, rellena valores faltantes en columnas categóricas y numéricas,
    convierte ciertas columnas a tipos de datos específicos, y formatea la columna 'volume_usd' 
    para evitar notación científica.

    Parámetros:
    - dataframe: El DataFrame con los datos a procesar.
    - diccionario_paises: Diccionario mapeando las distintas denominaciones de países, unificando en uno solo.
    
    Retorna:
    - dataframe: El DataFrame procesado con los cambios aplicados.
    """
    try:
        # Reemplaza los puntos y coma (;) por comas (,) en la columna 'country' para uniformizar el delimitador
        dataframe['country'] = dataframe['country'].str.replace(';', ',')
        # Divide las cadenas de texto de la columna 'country' en listas, usando la coma seguida de espacio como delimitador
        dataframe['country'] = dataframe['country'].str.split(', ')
        # "Explotar" la columna 'country', lo que convierte cada elemento de la lista en una fila separada, manteniendo los demás valores de la fila original (restablece los índices)
        dataframe = dataframe.explode('country').reset_index(drop=True)
        # Filtra las filas donde la columna 'country' no contiene "EU" o "NV"
        dataframe = dataframe[~dataframe['country'].isin(['EU', 'NV'])]

        # Reemplaza las celdas vacías con NaN para un manejo adecuado de los valores faltantes
        dataframe = dataframe.replace('', np.nan)   
            
        # Reemplaza los valores de la columna 'country' de acuerdo con un diccionario de países, que mapea los valores antiguos a sus nombres correctos
        dataframe['country'] = dataframe['country'].replace(diccionario_paises)
        
        # Itera sobre cada columna del DataFrame para realizar las transformaciones necesarias
        for column in dataframe.columns:
            # Si la columna es de tipo objeto (strings), maneja los valores faltantes
            if dataframe[column].dtype == 'object':
                # Rellena los valores NaN con 'Sin Dato' para tener un valor predeterminado
                dataframe[column] = dataframe[column].fillna('Sin Dato')
                
                # Si la columna es 'id' o 'name_id', cambia su tipo a 'category' para optimizar el uso de memoria
                if column in ['id','name_id'] and dataframe[column].dtype == 'object':
                    dataframe[column] = dataframe[column].astype('category')
            
            # Si la columna es numérica, maneja los valores faltantes
            elif np.issubdtype(dataframe[column].dtype, np.number):
                # Rellena los valores NaN con 0 en las columnas numéricas
                dataframe[column] = dataframe[column].fillna(0) 
                
                # Si la columna es 'volume_usd', asegura que sea de tipo float
                if column == 'volume_usd':
                    dataframe[column] = dataframe[column].astype(float)
        
        # Formatea la columna 'volume_usd' para evitar la notación científica y mostrar los valores con tres decimales
        dataframe['volume_usd'] = dataframe['volume_usd'].map(lambda x: '{:.3f}'.format(x)) 
        
        print('El procesamiento de datos full se realizó con éxito.')
        # Retorna el DataFrame procesado
        return dataframe
    
    except Exception as e:
        print(f'Error en el procesamiento de datos full: {e}')

# Función para procesar la data incremental
def procesamiento_datos_incremental(ruta_data, columnas_float, columnas_category=None):
    """
    Esta función procesa los datos de una tabla Delta, reemplaza los valores nulos y ajusta el tipo de datos de las columnas
    según las especificaciones proporcionadas. Las columnas indicadas como 'float' se convierten en números flotantes, 
    y las columnas 'category' se llenan con valores predeterminados.

    Parámetros:
    - ruta_data: Ruta de la tabla Delta que contiene los datos a procesar.
    - columnas_float: Lista de nombres de columnas que deben ser convertidas a tipo flotante.
    - columnas_category: Lista de nombres de columnas que deben ser convertidas a tipo categoría. Si no se proporciona, se omite.

    Retorna:
    - dataframe: El DataFrame procesado con las columnas ajustadas y los valores nulos reemplazados.
    """
    try:
        # Cargar la tabla Delta desde la ruta indicada
        data_delta = DeltaTable(ruta_data)
        dataframe = data_delta.to_pandas()
        
        # Reemplazar las cadenas vacías por NaN
        dataframe = dataframe.replace('', pd.NA)
        
        # Iterar sobre las columnas del DataFrame
        for column in dataframe.columns:
            # Si la columna está en la lista de columnas_float, procesarla como numérica
            if column in columnas_float:
                # Convertir la columna a tipo numérico (rellenar los NaN con 0) y redondear a 3 decimales
                dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce').fillna(0)
                dataframe[column] = dataframe[column].round(3)
                dataframe[column] = dataframe[column].astype(float)
            
            # Si la columna está en el DataFrame (aunque no esté en columnas_float), procesarla como categoría
            elif column in dataframe.columns:
                # Llenar los valores nulos con 'Sin Dato'
                dataframe[column] = dataframe[column].fillna('Sin Dato')
                
                # Comentado por razones de compatibilidad con Delta Lake (categorías no se leen bien en este formato)
                # dataframe[column] = dataframe[column].astype('category')
        
        print('El procesamiento de datos incrementales se realizó con éxito.')
        # Devolver el DataFrame procesado
        return dataframe
    
    except Exception as e:
        print(f'Error en el procesamiento de datos incrementales: {e}')

# Función para incluir columnas de agregación
def columnas_agregacion(ruta_data, columna, columnas_diff, columnas_cumsum):
    """
    Esta función realiza agregaciones en un DataFrame cargado desde una tabla Delta. Para las columnas especificadas en 
    'columnas_diff', calcula la diferencia respecto al valor anterior dentro de cada grupo definido por 'columna'. 
    Para las columnas en 'columnas_cumsum', calcula la suma acumulada dentro de cada grupo definido por 'columna'. 

    Parámetros:
    - ruta_data: Ruta de la tabla Delta que contiene los datos a procesar.
    - columna: Nombre de la columna que se usará para agrupar los datos.
    - columnas_diff: Lista de columnas para las cuales se calcularán las diferencias entre los valores consecutivos dentro de cada grupo.
    - columnas_cumsum: Lista de columnas para las cuales se calculará la suma acumulada dentro de cada grupo.

    Retorna:
    - dataframe: El DataFrame con las nuevas columnas de diferencia y suma acumulada agregadas.
    """
    try:
        # Cargar la tabla Delta desde la ruta indicada
        data_delta = DeltaTable(ruta_data)
        dataframe = data_delta.to_pandas()
        
        # Asegurarse de que columnas_diff y columnas_cumsum son listas, incluso si se pasan como cadenas
        if isinstance(columnas_diff, str):
            columnas_diff = [columnas_diff]
        
        if isinstance(columnas_cumsum, str):
            columnas_cumsum = [columnas_cumsum]
        
        # Ordenar el DataFrame por la columna de agrupación y las columnas de diferencia y suma acumulada
        dataframe = dataframe.sort_values(by=[columna] + columnas_diff + columnas_cumsum)
        
        # Calcular la diferencia entre el valor actual y el anterior dentro de cada grupo
        for column in columnas_diff:
            dataframe[f'diff_{column}'] = dataframe.groupby(columna)[column].diff().fillna(dataframe[column])
        
        # Calcular la suma acumulada dentro de cada grupo
        for column in columnas_cumsum:
            dataframe[f'cumsum_{column}'] = dataframe.groupby(columna)[column].cumsum()
        
        print('Las columnas de agregación se crearon con éxito.')
        # Devolver el DataFrame con las nuevas columnas
        return dataframe
    
    except Exception as e:
        print(f'Error en la creación de nuevas columnas de agregación: {e}')

# Función para guardar la data en formato DeltaLake
def guardar_data_delta(dataframe, ruta:str, mode='overwrite', partitions_col=None):
    """
    Esta función guarda un DataFrame en formato Delta Lake. Realiza una validación de los parámetros 
    y luego utiliza la función de escritura en Delta Lake para almacenar los datos en la ruta especificada.

    Parámetros:
    - dataframe: El DataFrame de Pandas que contiene los datos a guardar.
    - ruta: La ruta (URI o ubicación en el sistema de archivos) donde se debe guardar el archivo Delta Lake.
    - mode: El modo de escritura, puede ser 'overwrite', 'append', etc. El valor por defecto es 'overwrite'.
    - partitions_col: Una lista de columnas por las cuales particionar los datos en Delta Lake. Este parámetro es opcional.

    Retorna:
    - El resultado de la operación de escritura, generalmente una confirmación de que los datos fueron guardados correctamente.
    """
    
    # Validación de los tipos de los parámetros de entrada
    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError('El parámetro "dataframe" debe ser una instancia de DataFrame de Pandas.')
    if not isinstance(ruta, str):
        raise ValueError('El parámetro "ruta" debe ser de tipo "str".')
    
    try:
        if partitions_col is None:
            data_delta = write_deltalake(data=dataframe, table_or_uri=ruta, mode=mode)

        else:
            # Llama a la función de escritura en Delta Lake con los parámetros proporcionados
            data_delta = write_deltalake(data=dataframe, table_or_uri=ruta, mode=mode, partition_by=[partitions_col])
        
        print(f'Los datos del dataframe se guardaron con éxito en formato DeltaLake.')  
        # Retorna el resultado de la operación de escritura
        return data_delta
    
    except Exception as e:
        # Si ocurre un error durante la operación, lo captura y muestra un mensaje
        print(f'Error al intentar guardar los datos del dataframe en formato Delta Lake: {e}')

# Función para guardar nueva data, en formato DeltaLake, en una tabla existente
def guardar_nueva_data(ruta_actual_data, nueva_data, predicate, constraints=None, partitions_col=None):
    """
    Esta función guarda nuevos datos en una tabla Delta existente, ya sea mediante una inserción simple 
    (cuando no se especifica partición) o aplicando particiones y realizando una operación de fusión (merge).

    Parámetros:
    - ruta_actual_data: La ruta donde se encuentra la tabla Delta actual.
    - nueva_data: El nuevo DataFrame de Pandas con los datos que se agregarán a la tabla Delta.
    - predicate: Una condición de fusión que se utiliza para combinar los datos nuevos con los existentes.
    - constraints: Restricciones adicionales para el proceso de guardado (opcional, no usado en este ejemplo).
    - partitions_col: Una columna por la cual particionar los datos. Si no se proporciona, los datos se insertan sin partición.

    """
    
    try:
        # Verifica si la ruta proporcionada existe
        if not os.path.exists(ruta_actual_data):
            raise FileNotFoundError(f"La ruta {ruta_actual_data} no existe.")
        
        # Abre la tabla Delta en la ruta indicada
        actual_data = DeltaTable(ruta_actual_data)

        # Convierte el DataFrame de Pandas a formato Arrow (requerido por Delta Lake)
        nueva_data_pa = pa.Table.from_pandas(nueva_data)
        
        # Verifica si se debe aplicar partición
        if partitions_col:
            # Si se proporciona partición, usa el parámetro para escribir en particiones
            write_deltalake(
                ruta_actual_data,
                nueva_data_pa,
                mode='append',  # Modo de escritura "append" para agregar datos
                partition_by=[partitions_col]  # Particiona por la columna indicada
            )
        else:
            # Si no se especifica partición, realiza una operación de Merge
            actual_data.merge(
                source=nueva_data_pa,           # Fuente de los nuevos datos
                source_alias='source',          # Alias para la fuente
                target_alias='target',          # Alias para el destino
                predicate=predicate             # Condición de fusión (cómo se deben combinar los datos)
            ).when_not_matched_insert_all().execute()  # Inserta todos los nuevos registros que no coincidan
        print('Los datos nuevos se guardaron con éxito en una capa ya existente.')
        
    except Exception as e:
        # Captura cualquier error y muestra el mensaje
        print(f'Error al guardar la nueva data: {e}')


# COMO PLUS A LAS CONSIGNAS PRINCIPALES SE OPTÓ POR CREAR DOS FUNCIONES MAS, CON EL FIN DE MANEJAR "INFORMACIÓN CONFIDENCIAL".

# Función para crear correos electrónicos ficticios
def generar_correo_electronico(dataframe, nombre_columna, columna_email):
    """
    Genera una nueva columna de correos electrónicos ficticios.

    Esta función utiliza la biblioteca Faker para crear una parte local aleatoria del correo (antes del '@')
    y combina esta parte con un dominio basado en otra columna del DataFrame (nombre_columna).
    
    Parameters:
        dataframe (pd.DataFrame): DataFrame que contiene los datos originales.
        nombre_columna (str): Nombre de la columna que contiene los nombres para crear el dominio.
        columna_email (str): Nombre de la nueva columna para almacenar los correos generados.
    
    Returns:
        pd.DataFrame: El DataFrame original con una nueva columna que contiene los correos generados.
    """
    
    try:
        faker = Faker()  # Instancia de Faker para generar datos ficticios
        emails = []  # Lista para almacenar los correos generados

        # Iterar por cada fila del DataFrame
        for _, fila in dataframe.iterrows():
            local_part = faker.email().split('@')[0]  # Generar la parte antes del '@'
            dominio = f'{fila[nombre_columna].lower()}.org'  # Crear dominio basado en la columna 'nombre_columna'
            emails.append(f'{local_part}@{dominio}')  # Combinar la parte local y el dominio para formar el correo
        
        # Agregar la lista de correos como una nueva columna en el DataFrame
        dataframe[columna_email] = emails
        
        print('Se creó con éxito la columna ficticia de correo electrónicos.')
        return dataframe  # Devolver el DataFrame actualizado
    
    except Exception as e:
        print(f'Error en la creación de la columna ficticia de correos electrónicos: {e}')

# Función para enmascarar la información del correo
def enmascarar_emails(dataframe, columna_email_original, columna_email_enmascarada):
    """
    Enmascara los correos electrónicos de una columna, ocultando la parte antes del '@'.

    Esta función toma una columna que contiene correos electrónicos, extrae el dominio
    (parte después del '@') y reemplaza la parte anterior con '*****'. Crea una nueva columna
    con los correos enmascarados y elimina la columna original.

    Parameters:
        dataframe (pd.DataFrame): DataFrame que contiene los datos originales.
        columna_email_original (str): Nombre de la columna con los correos electrónicos originales.
        columna_email_enmascarada (str): Nombre de la nueva columna para almacenar los correos enmascarados.
    
    Returns:
        pd.DataFrame: El DataFrame con la nueva columna de correos enmascarados.
    """
    try: 
        enmascarados = []  # Lista para almacenar los correos enmascarados

        # Iterar por cada correo en la columna original
        for email in dataframe[columna_email_original]:
            dominio = email.split('@')[1]  # Extraer el dominio (parte después del '@')
            enmascarados.append(f'*****@{dominio}')  # Crear el correo enmascarado
        
        # Agregar la lista de correos enmascarados como una nueva columna
        dataframe[columna_email_enmascarada] = enmascarados

        # Eliminar la columna original del DataFrame para evitar duplicados
        dataframe.drop(columns=[columna_email_original], inplace=True)
        
        print('Enmascaramiento exitoso.')
        return dataframe  # Devolver el DataFrame actualizado
    
    except Exception as e:
        print(f'Error en el enmascaramiento: {e}')



# DATOS DE CONEXIÓN A LA API
base_url = 'https://api.coinlore.net/api/{}'
endpoint_temporal = 'ticker'
endpoint_estatico = 'exchanges'
url = base_url.format(endpoint_temporal)

params_temporal = {
    'id' : ",".join(['122', '95', '106'])
}


# LISTAS DE COLUMNAS PARA MANEJAR SUS DATOS CON MAYOR FACILIDAD
columnas_incremental_float = ['price_usd', 'percent_change_24h', 'percent_change_1h', 'percent_change_7d', 'price_btc', 'market_cap_usd', 'volume24', 'volume24a', 'csupply', 'tsupply', 'msupply']
# No permite leer el archivo DeltaLake si posee columnas tipo "category". Por lo que las columnas detallas en la línea siguiente, se optó por dejarlas como "object"
columnas_incremental_category = ['id', 'symbol', 'name', 'nameid', 'rank']

# Diccionario para mapear las distintas nominaciones de países
normalizaciones_paises = {
    'British Virgin Islands' : 'United Kingdom',
    'Cayman Islands' : 'United Kingdom',
    'London' : 'United Kingdom',
    'HK' : 'United Kingdom',
    'UK' : 'United Kingdom',
    'San Francisco' : 'United States', 
    'California' : 'United States',
    'Las Vegas' : 'United States',
    'US' : 'United States',
    'Virginia' : 'United States',
    'Wilmington' : 'United States', 
    'Delaware' : 'United States',
    'Chiba-cho' : 'Japan', 
    'Nihonbashi' : 'Japan', 
    'Chuo-ku' : 'Japan',
    'Tokyo' : 'Japan',
    'Hong Kong' : 'Japan',
    }


# -------------------------
# SE APLICAN AMBAS EXTRACCIONES (INCREMENTAL Y FULL)
dataframe_incremental = aplicar_extraccion_incremental(base_url, endpoint_temporal, params=params_temporal)
dataframe_estatico = aplicar_extraccion_full(base_url, endpoint_estatico)

# SE GUARDA LA DATA EXTRAÍDA EN FORMATO DELTA LAKE (BRONZE)
guardar_data_delta(dataframe_incremental, 'Datos_Delta/Bronze/CoinLore/ticker', mode='append')
guardar_data_delta(dataframe_estatico, 'Datos_Delta/Bronze/CoinLore/exchanges')

# EN CASO DE GUARDAR NUEVOS DATOS "BRONZE" EN UNA RUTA YA EXISTENTE UTILIZAMOS LA SIGUIENTE FUNCIÓN:
# guardar_nueva_data('Datos_Delta/Bronze/CoinLore/ticker', dataframe_incremental, 'source.id = target.id' )
# guardar_nueva_data('Datos_Delta/Bronze/CoinLore/exchanges', dataframe_incremental, 'source.id = target.id', partitions_col='country' )


# -------------------------
# PROCESAMOS LOS DATOS 
dataframe_incremental_procesado = procesamiento_datos_incremental('Datos_Delta/Bronze/CoinLore/ticker', columnas_incremental_float, columnas_incremental_category)
dataframe_estatico_procesado = procesamiento_datos_full(procesamiento_melt_datos_full(dataframe_estatico), normalizaciones_paises)

# SE APLICA LA CREACIÓN Y ENMASCARAMIENTO DE EMAILS FICTICIOS
dataframe_estatico_procesado_email = generar_correo_electronico(dataframe_estatico_procesado, 'name_id', 'email')
# SE CREA UNA COPIA DEL DF ANTES DE ENMASCARAR
dataframe_confidential = dataframe_estatico_procesado_email.copy()
dataframe_estatico_procesado_email_enmascarado = enmascarar_emails(dataframe_estatico_procesado_email, 'email', 'email_enmascarados')

# SE GUARDA LA DATA "CONFIDENCIAL" EN UNA NUEVA CAPA DELTA (CONFIDENTIAL)
guardar_data_delta(dataframe_confidential, 'Datos_Delta/Confidential/CoinLore/exchanges')

# SE GUARDA LA DATA PROCESADA EN UNA NUEVA CAPA DELTA LAKE (SILVER)
guardar_data_delta(dataframe_incremental_procesado, 'Datos_Delta/Silver/CoinLore/ticker', mode='append')
guardar_data_delta(dataframe_estatico_procesado_email_enmascarado, 'Datos_Delta/Silver/CoinLore/exchanges', partitions_col='country')

# EN CASO DE GUARDAR NUEVOS DATOS "SIVLER" EN UNA RUTA YA EXISTENTE UTILIZAMOS LA SIGUIENTE FUNCIÓN:
# guardar_nueva_data('Datos_Delta/Silver/CoinLore/ticker', dataframe_incremental_procesado, 'source.id = target.id' )
# guardar_nueva_data('Datos_Delta/Silver/CoinLore/exchanges', dataframe_incremental_procesado, 'source.id = target.id', partitions_col='country')


# -------------------------
# SE INCLUYEN COLUMNAS DE AGREGACIÓN A LA DATA INCREMENTAL (NO SE APLICA A LA DATA FULL YA QUE SE CONSIDERA QUE NO HAY VARIABILIDAD DE DATOS)
dataframe_incremental_agregacion = columnas_agregacion('Datos_Delta/Silver/CoinLore/ticker', 'id', ['market_cap_usd', 'volume24'], 'volume24a')

# SE GUARDA LA NUEVA DATA INCREMENTAL, CON AGREGACIONES REALIZADAS, EN LA CAPA GOLD
guardar_data_delta(dataframe_incremental_agregacion, 'Datos_Delta/Gold/CoinLore/ticker', mode='append')

# EN CASO DE GUARDAR NUEVOS DATOS "GOLD" EN UNA RUTA YA EXISTENTE UTILIZAMOS LA SIGUIENTE FUNCIÓN:
#guardar_nueva_data('Datos_Delta/Gold/CoinLore/ticker', dataframe_incremental_agregacion, 'source.id = target.id' )
