# TrabajoGrado

Este repositorio contiene todos los archivos y códigos necesarios para el desarrollo del proyecto “Modelo prescriptivo para la mejora del score crediticio personalizado”, cuyo objetivo es recomendar acciones concretas y personalizadas a los clientes bancarios para mejorar su score crediticio. A través de técnicas de aprendizaje automático, el modelo busca anticipar el impacto de decisiones financieras individuales, optimizando así la gestión del riesgo por parte de las entidades y fortaleciendo la relación con sus usuarios.

# Carpeta SQL:
contiene todas las consultas necesarias para la creación y estructuración de la base de datos utilizada en el proyecto. Los scripts están organizados y enumerados en el orden en que deben ser ejecutados, lo cual refleja el proceso de construcción progresiva de la tabla principal. Estas consultas permiten extraer y consolidar toda la información transaccional relevante del cliente en el banco, que sirve como base para las siguientes etapas del análisis y modelado.

# Ejecución de SQL:
Los scripts deben ejecutarse en el orden numérico indicado (por ejemplo, 01_base_clientes_objetivo.sql, 02_base_desempleo.sql, etc.) dentro de un entorno de base de datos compatible con SQL estándar (como PostgreSQL, MySQL o SQL Server, según corresponda). Además de la creación de las tablas base, estas consultas SQL incluyen una parte fundamental del preprocesamiento de los datos, como la limpieza, transformación de variables y generación de variables derivadas, lo que permite estructurar una base sólida y lista para las etapas posteriores de análisis y modelado.

# Carpeta Data:
La carpeta Data contiene las tablas resultantes de la ejecución de los scripts SQL, almacenadas en formato .parquet para garantizar eficiencia en el almacenamiento y procesamiento. Estos archivos representan la base de datos consolidada con toda la información transaccional del cliente, y están nombrados y enumerados siguiendo el orden en que fueron creados, lo que facilita su seguimiento y uso posterior. Originalmente, la base de datos contenía alrededor de 740 millones de registros, por lo que se optó por tomar una muestra aleatoria representativa de aproximadamente 1.500.000 registros para facilitar el desarrollo y análisis. Debido a las limitaciones de tamaño impuestas por GitHub, esta muestra fue dividida en 11 partes, por lo que la tabla final se encuentra fragmentada en múltiples archivos dentro de esta carpeta.

# Carpeta Code:
contiene los notebooks que conforman la ejecución central del proyecto. En esta etapa de desarrollo, el flujo abarca la ingesta de los datos previamente construidos, la exploración gráfica de las variables mediante histogramas y boxplots para la detección de outliers, el análisis de correlación entre variables, y la limpieza y preparación de los datos, enfocándose principalmente en el tratamiento de valores atípicos y nulos. Finalmente, se realiza la visualización de los resultados obtenidos. La ejecución de los notebooks debe seguir el orden establecido en sus nombres, ya que cada uno representa un paso secuencial dentro del proceso de análisis y modelado.


# Requisitos del entorno y ejecución del proyecto

Para ejecutar este proyecto de forma local, se debe contar con los siguientes elementos instalados:

# 1. Requisitos

- Python 3.8 o superior
- Gestor de paquetes pip o conda

# Las siguientes librerías de Python (pueden instalarse con pip install o desde un requirements.txt):

pip install pandas numpy matplotlib seaborn plotly pyarrow

# 2. Estructura del proyecto

El repositorio está organizado en las siguientes carpetas:
- SQL/ → Contiene scripts para crear y poblar las tablas base.
- DATA/ → Contiene los datos exportados en formato .parquet, generados desde los scripts SQL.
- Code/ → Contiene los notebooks que ejecutan el flujo de análisis y modelado, organizados de forma secuencial.

# 3. Ejecución
- Base de datos: Ejecutar los scripts en la carpeta SQL en el orden indicado para generar la base de datos.
- Datos: Si ya se cuenta con los archivos .parquet en la carpeta DATA, pse puede avanzar directamente a los notebooks.
- Notebooks: Abrir los notebooks de la carpeta Code en Jupyter Notebook, JupyterLab o google collab. Ejecutarlos en el orden numérico establecido, ya que representan el paso a paso del proyecto.

