# TrabajoGrado

Este repositorio contiene todos los archivos y códigos necesarios para el desarrollo del proyecto “Modelo prescriptivo para la mejora del score crediticio personalizado”, cuyo objetivo es recomendar acciones concretas y personalizadas a los clientes bancarios para mejorar su score crediticio. A través de técnicas de aprendizaje automático, el modelo busca anticipar el impacto de decisiones financieras individuales, optimizando así la gestión del riesgo por parte de las entidades y fortaleciendo la relación con sus usuarios.

# Carpeta SQL:
contiene todas las consultas necesarias para la creación y estructuración de la base de datos utilizada en el proyecto. Los scripts están organizados y enumerados en el orden en que deben ser ejecutados, lo cual refleja el proceso de construcción progresiva de la tabla principal. Estas consultas permiten extraer y consolidar toda la información transaccional relevante del cliente en el banco, que sirve como base para las siguientes etapas del análisis y modelado.

# Ejecución:
Los scripts deben ejecutarse en el orden numérico indicado (por ejemplo, 01_base_clientes_objetivo.sql, 02_base_desempleo.sql, etc.) dentro de un entorno de base de datos compatible con SQL estándar (como PostgreSQL, MySQL o SQL Server, según corresponda).

# Carpeta Data:
Contiene las tablas resultantes de la ejecución de los scripts SQL, almacenadas en formato .parquet para garantizar eficiencia en el almacenamiento y procesamiento. Estos archivos representan la base de datos consolidada con toda la información transaccional del cliente, y están nombrados y enumerados siguiendo el orden en que fueron creados, lo que facilita su seguimiento y uso posterior. Estos datos sirven como insumo principal para las etapas de análisis exploratorio, modelado y validación del proyecto.

# Carpeta Code:
contiene los notebooks que conforman la ejecución central del proyecto. En esta etapa de desarrollo, el flujo abarca la ingesta de los datos previamente construidos, la exploración gráfica de las variables mediante histogramas y boxplots para la detección de outliers, el análisis de correlación entre variables, y la limpieza y preparación de los datos, enfocándose principalmente en el tratamiento de valores atípicos y nulos. Finalmente, se realiza la visualización de los resultados obtenidos. La ejecución de los notebooks debe seguir el orden establecido en sus nombres, ya que cada uno representa un paso secuencial dentro del proceso de análisis y modelado.
