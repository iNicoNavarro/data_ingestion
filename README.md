# Proyecto de Ingestión y Limpieza de Datos - Series de TV

Este proyecto tiene como objetivo construir un pipeline de ingestión y limpieza de datos para recopilar información sobre series de televisión, analizar su calidad mediante un reporte de profiling, limpiar los datos y almacenarlos en un archivo Parquet comprimido. Finalmente, se carga esta información en una base de datos SQLite para facilitar su consulta.

## Estructura del Proyecto

```bash
data_ingestion/
├── data/                       # Carpeta para archivos Parquet
│   └── series_tv_data.parquet  # Archivo Parquet comprimido con los datos procesados
├── db/                         # Carpeta para la base de datos
│   └── series_tv.db            # Base de datos SQLite
├── json/                       # Carpeta para almacenar los archivos JSON
│   ├── series_2024-01-01_2024-01-07.json
│   ├── series_2024-01-08_2024-01-14.json
│   └── ...
├── model/                      # Carpeta para reportes de profiling
│   ├── reporte_series_tv.html  # Reporte en HTML
│   └── reporte_series_tv.pdf   # Reporte en PDF
├── src/                        # Código fuente del proyecto
│   ├── main.py                 # Script principal para ejecutar el flujo de trabajo
│   └── core.py                 # Funciones auxiliares para el procesamiento de datos
└── README.md                   # Este archivo de documentación
```

## Requisitos

* Python 3.8+
* Librerías de Python (instalar con `pip install -r requirements.txt`):
  * `pandas`
  * `requests`
  * `ydata-profiling`
  * `pyarrow`
  * `pdfkit`
  * [wkhtmltopdf](https://wkhtmltopdf.org/) para la conversión de HTML a PDF.

## Configuración

Asegúrate de instalar `wkhtmltopdf` y configurarlo para que `pdfkit` pueda generar el reporte de profiling en formato PDF.

## Funcionalidades

### 1. Ingesta de Datos desde una API

En el archivo `main.py`, la función `fetch_and_upload` obtiene datos de una API externa (TVMaze) y los almacena en archivos JSON semanales en la carpeta `json/`. Los datos se guardan en un archivo diferente para cada semana del mes de enero de 2024.

<pre class="!overflow-visible"><div class="contain-inline-size rounded-md border-[0.5px] border-token-border-medium relative bg-token-sidebar-surface-primary dark:bg-gray-950"><div class="flex items-center text-token-text-secondary px-4 py-2 text-xs font-sans justify-between rounded-t-md h-9 bg-token-sidebar-surface-primary dark:bg-token-main-surface-secondary">python</div><div class="sticky top-9 md:top-[5.75rem]"><div class="absolute bottom-0 right-2 flex h-9 items-center"><div class="flex items-center rounded bg-token-sidebar-surface-primary px-2 font-sans text-xs text-token-text-secondary dark:bg-token-main-surface-secondary"><span class="" data-state="closed"><button class="flex gap-1 items-center py-1"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="icon-sm"><path fill-rule="evenodd" clip-rule="evenodd" d="M7 5C7 3.34315 8.34315 2 10 2H19C20.6569 2 22 3.34315 22 5V14C22 15.6569 20.6569 17 19 17H17V19C17 20.6569 15.6569 22 14 22H5C3.34315 22 2 20.6569 2 19V10C2 8.34315 3.34315 7 5 7H7V5ZM9 7H14C15.6569 7 17 8.34315 17 10V15H19C19.5523 15 20 14.5523 20 14V5C20 4.44772 19.5523 4 19 4H10C9.44772 4 9 4.44772 9 5V7ZM5 9C4.44772 9 4 9.44772 4 10V19C4 19.5523 4.44772 20 5 20H14C14.5523 20 15 19.5523 15 19V10C15 9.44772 14.5523 9 14 9H5Z" fill="currentColor"></path></svg>Copiar código</button></span></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="!whitespace-pre hljs language-python">fetch_and_upload(
    start_date=START_DATE, 
    end_date=END_DATE
)
</code></div></div></pre>

### 2. Normalización y Generación de DataFrame

La función `load_and_normalize_json_files` en `core.py` carga todos los archivos JSON generados y normaliza la estructura de datos para formar un único DataFrame.

### 3. Generación de un Reporte de Profiling

Se genera un reporte de calidad de los datos utilizando `ydata-profiling`. Este reporte se guarda en formato HTML y PDF en la carpeta `model/`. Los detalles del profiling se pueden utilizar para decidir qué columnas limpiar o eliminar.

<pre class="!overflow-visible"><div class="contain-inline-size rounded-md border-[0.5px] border-token-border-medium relative bg-token-sidebar-surface-primary dark:bg-gray-950"><div class="flex items-center text-token-text-secondary px-4 py-2 text-xs font-sans justify-between rounded-t-md h-9 bg-token-sidebar-surface-primary dark:bg-token-main-surface-secondary">python</div><div class="sticky top-9 md:top-[5.75rem]"><div class="absolute bottom-0 right-2 flex h-9 items-center"><div class="flex items-center rounded bg-token-sidebar-surface-primary px-2 font-sans text-xs text-token-text-secondary dark:bg-token-main-surface-secondary"><span class="" data-state="closed"><button class="flex gap-1 items-center py-1"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="icon-sm"><path fill-rule="evenodd" clip-rule="evenodd" d="M7 5C7 3.34315 8.34315 2 10 2H19C20.6569 2 22 3.34315 22 5V14C22 15.6569 20.6569 17 19 17H17V19C17 20.6569 15.6569 22 14 22H5C3.34315 22 2 20.6569 2 19V10C2 8.34315 3.34315 7 5 7H7V5ZM9 7H14C15.6569 7 17 8.34315 17 10V15H19C19.5523 15 20 14.5523 20 14V5C20 4.44772 19.5523 4 19 4H10C9.44772 4 9 4.44772 9 5V7ZM5 9C4.44772 9 4 9.44772 4 10V19C4 19.5523 4.44772 20 5 20H14C14.5523 20 15 19.5523 15 19V10C15 9.44772 14.5523 9 14 9H5Z" fill="currentColor"></path></svg>Copiar código</button></span></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="!whitespace-pre hljs language-python">core.generate_profiling_report(
    df=df_raw,
    output_folder=PATH__PROFILE,
    filename="reporte_series_tv.html"
)
</code></div></div></pre>

### 4. Limpieza de Datos

En base al reporte de profiling, la función `clean_dataframe` realiza las siguientes acciones de limpieza en el DataFrame:

* Eliminación de columnas con más del 70% de valores nulos.
* Conversión de campos de fecha (`airdate`) al tipo `datetime` en formato de solo fecha.

<pre class="!overflow-visible"><div class="contain-inline-size rounded-md border-[0.5px] border-token-border-medium relative bg-token-sidebar-surface-primary dark:bg-gray-950"><div class="flex items-center text-token-text-secondary px-4 py-2 text-xs font-sans justify-between rounded-t-md h-9 bg-token-sidebar-surface-primary dark:bg-token-main-surface-secondary">python</div><div class="sticky top-9 md:top-[5.75rem]"><div class="absolute bottom-0 right-2 flex h-9 items-center"><div class="flex items-center rounded bg-token-sidebar-surface-primary px-2 font-sans text-xs text-token-text-secondary dark:bg-token-main-surface-secondary"><span class="" data-state="closed"><button class="flex gap-1 items-center py-1"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="icon-sm"><path fill-rule="evenodd" clip-rule="evenodd" d="M7 5C7 3.34315 8.34315 2 10 2H19C20.6569 2 22 3.34315 22 5V14C22 15.6569 20.6569 17 19 17H17V19C17 20.6569 15.6569 22 14 22H5C3.34315 22 2 20.6569 2 19V10C2 8.34315 3.34315 7 5 7H7V5ZM9 7H14C15.6569 7 17 8.34315 17 10V15H19C19.5523 15 20 14.5523 20 14V5C20 4.44772 19.5523 4 19 4H10C9.44772 4 9 4.44772 9 5V7ZM5 9C4.44772 9 4 9.44772 4 10V19C4 19.5523 4.44772 20 5 20H14C14.5523 20 15 19.5523 15 19V10C15 9.44772 14.5523 9 14 9H5Z" fill="currentColor"></path></svg>Copiar código</button></span></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="!whitespace-pre hljs language-python">df_cleaned = core.clean_dataframe(df=df_raw)
</code></div></div></pre>

### 5. Almacenamiento en Formato Parquet

Después de limpiar los datos, el DataFrame resultante se guarda en un archivo Parquet con compresión Snappy. Esto optimiza el almacenamiento y permite una rápida lectura en otros procesos.

<pre class="!overflow-visible"><div class="contain-inline-size rounded-md border-[0.5px] border-token-border-medium relative bg-token-sidebar-surface-primary dark:bg-gray-950"><div class="flex items-center text-token-text-secondary px-4 py-2 text-xs font-sans justify-between rounded-t-md h-9 bg-token-sidebar-surface-primary dark:bg-token-main-surface-secondary">python</div><div class="sticky top-9 md:top-[5.75rem]"><div class="absolute bottom-0 right-2 flex h-9 items-center"><div class="flex items-center rounded bg-token-sidebar-surface-primary px-2 font-sans text-xs text-token-text-secondary dark:bg-token-main-surface-secondary"><span class="" data-state="closed"><button class="flex gap-1 items-center py-1"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="icon-sm"><path fill-rule="evenodd" clip-rule="evenodd" d="M7 5C7 3.34315 8.34315 2 10 2H19C20.6569 2 22 3.34315 22 5V14C22 15.6569 20.6569 17 19 17H17V19C17 20.6569 15.6569 22 14 22H5C3.34315 22 2 20.6569 2 19V10C2 8.34315 3.34315 7 5 7H7V5ZM9 7H14C15.6569 7 17 8.34315 17 10V15H19C19.5523 15 20 14.5523 20 14V5C20 4.44772 19.5523 4 19 4H10C9.44772 4 9 4.44772 9 5V7ZM5 9C4.44772 9 4 9.44772 4 10V19C4 19.5523 4.44772 20 5 20H14C14.5523 20 15 19.5523 15 19V10C15 9.44772 14.5523 9 14 9H5Z" fill="currentColor"></path></svg>Copiar código</button></span></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="!whitespace-pre hljs language-python">core.save_to_parquet(
    df=df_cleaned, 
    folder=PATH_PARQUET, 
    filename="series_tv_data.parquet"
)
</code></div></div></pre>

### 6. Carga en Base de Datos SQLite

El archivo Parquet es cargado en una base de datos SQLite. Se utiliza un modelo de datos que respeta la integridad de la información. La base de datos se guarda en la carpeta `db/` bajo el nombre `series_tv.db`.

<pre class="!overflow-visible"><div class="contain-inline-size rounded-md border-[0.5px] border-token-border-medium relative bg-token-sidebar-surface-primary dark:bg-gray-950"><div class="flex items-center text-token-text-secondary px-4 py-2 text-xs font-sans justify-between rounded-t-md h-9 bg-token-sidebar-surface-primary dark:bg-token-main-surface-secondary">python</div><div class="sticky top-9 md:top-[5.75rem]"><div class="absolute bottom-0 right-2 flex h-9 items-center"><div class="flex items-center rounded bg-token-sidebar-surface-primary px-2 font-sans text-xs text-token-text-secondary dark:bg-token-main-surface-secondary"><span class="" data-state="closed"><button class="flex gap-1 items-center py-1"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="icon-sm"><path fill-rule="evenodd" clip-rule="evenodd" d="M7 5C7 3.34315 8.34315 2 10 2H19C20.6569 2 22 3.34315 22 5V14C22 15.6569 20.6569 17 19 17H17V19C17 20.6569 15.6569 22 14 22H5C3.34315 22 2 20.6569 2 19V10C2 8.34315 3.34315 7 5 7H7V5ZM9 7H14C15.6569 7 17 8.34315 17 10V15H19C19.5523 15 20 14.5523 20 14V5C20 4.44772 19.5523 4 19 4H10C9.44772 4 9 4.44772 9 5V7ZM5 9C4.44772 9 4 9.44772 4 10V19C4 19.5523 4.44772 20 5 20H14C14.5523 20 15 19.5523 15 19V10C15 9.44772 14.5523 9 14 9H5Z" fill="currentColor"></path></svg>Copiar código</button></span></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="!whitespace-pre hljs language-python">core.load_to_sqlite(
    parquet_path=os.path.join(PATH_PARQUET, "series_tv_data.parquet"),
    db_path="../db/series_tv.db"
)
</code></div></div></pre>

## Uso del Script

Para ejecutar el pipeline de principio a fin, simplemente ejecuta `main.py`. Esto realizará la ingesta, limpieza, profiling, almacenamiento en Parquet y carga en SQLite:

<pre class="!overflow-visible"><div class="contain-inline-size rounded-md border-[0.5px] border-token-border-medium relative bg-token-sidebar-surface-primary dark:bg-gray-950"><div class="flex items-center text-token-text-secondary px-4 py-2 text-xs font-sans justify-between rounded-t-md h-9 bg-token-sidebar-surface-primary dark:bg-token-main-surface-secondary">bash</div><div class="sticky top-9 md:top-[5.75rem]"><div class="absolute bottom-0 right-2 flex h-9 items-center"><div class="flex items-center rounded bg-token-sidebar-surface-primary px-2 font-sans text-xs text-token-text-secondary dark:bg-token-main-surface-secondary"><span class="" data-state="closed"><button class="flex gap-1 items-center py-1"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="icon-sm"><path fill-rule="evenodd" clip-rule="evenodd" d="M7 5C7 3.34315 8.34315 2 10 2H19C20.6569 2 22 3.34315 22 5V14C22 15.6569 20.6569 17 19 17H17V19C17 20.6569 15.6569 22 14 22H5C3.34315 22 2 20.6569 2 19V10C2 8.34315 3.34315 7 5 7H7V5ZM9 7H14C15.6569 7 17 8.34315 17 10V15H19C19.5523 15 20 14.5523 20 14V5C20 4.44772 19.5523 4 19 4H10C9.44772 4 9 4.44772 9 5V7ZM5 9C4.44772 9 4 9.44772 4 10V19C4 19.5523 4.44772 20 5 20H14C14.5523 20 15 19.5523 15 19V10C15 9.44772 14.5523 9 14 9H5Z" fill="currentColor"></path></svg>Copiar código</button></span></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="!whitespace-pre hljs language-bash">python3 src/main.py
</code></div></div></pre>

## Funciones en `core.py`

* **fetch_series_by_date** : Llama a la API de TVMaze para obtener los datos de series de televisión por fecha.
* **save_json** : Guarda los datos en formato JSON en la carpeta `json/`.
* **load_and_normalize_json_files** : Carga y normaliza los archivos JSON en un DataFrame.
* **generate_profiling_report** : Genera un reporte de profiling en HTML y PDF.
* **clean_dataframe** : Realiza la limpieza de columnas con base en el porcentaje de valores nulos y convierte fechas al tipo adecuado.
* **save_to_parquet** : Guarda el DataFrame limpio en formato Parquet con compresión Snappy.
* **load_to_sqlite** : Carga el archivo Parquet en una base de datos SQLite respetando la integridad de los datos.


# Modelo de datos

### 1. Tabla `shows`

* **Descripción** : Almacena la información principal de cada show de TV.
* **Campos** :
* `id` (INTEGER, PRIMARY KEY): Identificador único del show.
* `name` (TEXT): Nombre del show.
* `type` (TEXT): Tipo de show (e.g., Scripted).
* `language` (TEXT): Idioma del show.
* `status` (TEXT): Estado del show (e.g., Running, Ended).
* `averageRuntime` (REAL): Tiempo promedio de duración de cada episodio.
* `premiered` (DATE): Fecha de estreno del show.
* `ended` (DATE): Fecha de finalización del show.
* `officialSite` (TEXT): URL del sitio oficial.
* `weight` (INTEGER): Peso del show (puede indicar popularidad o importancia en TVMaze).

### 2. Tabla `episodes`

* **Descripción** : Contiene los detalles de cada episodio de un show.
* **Campos** :
* `id` (INTEGER, PRIMARY KEY): Identificador único del episodio.
* `url` (TEXT): URL del episodio en TVMaze.
* `name` (TEXT): Nombre del episodio.
* `season` (INTEGER): Temporada del episodio.
* `number` (REAL): Número del episodio en la temporada.
* `type` (TEXT): Tipo de episodio (e.g., regular).
* `airdate` (DATE): Fecha de emisión.
* `runtime` (REAL): Duración del episodio en minutos.
* `show_id` (INTEGER, FOREIGN KEY a `shows(id)`): Identificador del show al que pertenece el episodio.

### 3. Tabla `genres`

* **Descripción** : Lista de géneros disponibles para los shows.
* **Campos** :
* `id` (INTEGER, PRIMARY KEY): Identificador único del género.
* `genre` (TEXT): Nombre del género (e.g., Drama, Comedy).

### 4. Tabla `show_genres`

* **Descripción** : Tabla intermedia que asocia shows con sus géneros (para manejar la relación de muchos a muchos entre `shows` y `genres`).
* **Campos** :
* `show_id` (INTEGER, FOREIGN KEY a `shows(id)`): Identificador del show.
* `genre_id` (INTEGER, FOREIGN KEY a `genres(id)`): Identificador del género.

### 5. Tabla `schedule`

* **Descripción** : Almacena el horario y días de emisión de cada show.
* **Campos** :
* `show_id` (INTEGER, FOREIGN KEY a `shows(id)`): Identificador del show.
* `time` (TEXT): Hora de emisión.
* `days` (TEXT): Días de emisión en formato de texto (e.g., '["Monday", "Tuesday"]').

---

### Relaciones entre Tablas

1. **`shows` ↔ `episodes`** : Relación de uno a muchos (un show tiene muchos episodios).

* Se conecta a través del campo `show_id` en la tabla `episodes`, que es una clave foránea hacia `shows(id)`.

1. **`shows` ↔ `genres` (a través de `show_genres`)** : Relación de muchos a muchos (un show puede tener múltiples géneros y un género puede estar en múltiples shows).

* La tabla `show_genres` es la tabla intermedia que asocia `show_id` con `genre_id`.

1. **`shows` ↔ `schedule`** : Relación de uno a uno o uno a muchos (un show puede tener un solo horario de emisión o varios si los horarios varían).

* La tabla `schedule` asocia el `show_id` con el `time` y `days`.

## Notas Adicionales

* **Compresión Snappy** : Mejora el rendimiento al almacenar los datos en Parquet.
* **SQLite** : Se elige SQLite como base de datos por su facilidad de configuración y portabilidad.
* **Reporte de Profiling** : El archivo `reporte_series_tv.html` (y su versión PDF) proporciona una visión detallada de la calidad de los datos, permitiendo decisiones informadas en la limpieza de datos.

## Consideraciones

Para evitar múltiples llamadas innecesarias a la API, se recomienda descomentar el bloque `fetch_and_upload` solo si es necesario obtener datos actualizados.

## Créditos

Proyecto desarrollado como parte de una práctica de Data Engineering.
