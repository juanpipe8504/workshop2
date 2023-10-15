# workshop2
# ETL Pipeline con Airflow

Este repositorio contiene el código para una canalización ETL (Extract, Transform, Load) implementada utilizando Apache Airflow. La canalización procesa datos de diferentes fuentes, aplica transformaciones y carga los datos transformados en un sistema de almacenamiento.

## Archivos y Componentes

- `base_etl.py`: Contiene funciones para extraer y transformar datos apartir de la base de datos.
- `csv_etl.py`: Incluye funciones para leer archivos CSV y realizar transformaciones específicas para datos en formato CSV.
- `merge.py`: Define funciones para fusionar datos de varias fuentes y cargarlos en un sistema de almacenamiento.
- `main_dag.py`: Contiene la definición de DAG de Airflow para orquestar la canalización ETL.

## Configuración del DAG de Airflow

1. Instala las dependencias necesarias 
2. Asegúrate de tener Apache Airflow instalado en tu sistema.
3. Importa los módulos y operadores necesarios para el DAG en `main_dag.py`.
4. Define los argumentos predeterminados para el DAG, incluyendo el propietario, la fecha de inicio, la configuración de correo electrónico, etc.
5. Define funciones en Python para las operaciones ETL, como leer datos, transformarlos y fusionarlos.
6. Configura las tareas utilizando PythonOperator para ejecutar las funciones ETL como tareas en el DAG.
7. Define las dependencias entre tareas para orquestar el flujo de trabajo ETL.

## Ejecución del DAG

Para ejecutar el DAG, asegúrate de tener Apache Airflow configurado correctamente. Coloca el archivo `main_dag.py` en el directorio de DAG de Airflow.

1. Inicia el planificador de Airflow: `airflow scheduler`
2. Inicia el servidor web de Airflow: `airflow 8080`
3. Accede a la interfaz web de Airflow y desencadena el DAG 'api__project_dag'.

El DAG se ejecutará según el horario especificado, realizando las operaciones ETL como se define en las tareas.


