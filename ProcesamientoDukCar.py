from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType

# 1. Inicializar sesión de Spark
spark = SparkSession.builder \
    .appName("ProcesamientoDukCar") \
    .getOrCreate()

# 2. Definir esquema para mejorar la lectura
schema = StructType([
    StructField("ID_Ingreso", IntegerType(), True),
    StructField("Fecha_Registro", StringType(), True),
    StructField("Nombre_Aseguradora", StringType(), True),
    StructField("Numero_Poliza", StringType(), True),
    StructField("Fecha_Expedicion_Poliza", StringType(), True),
    StructField("Nombre_Asesor_Comercial", StringType(), True),
    StructField("Nombre_Cliente", StringType(), True),
    StructField("Valor_Prima_Neta", DoubleType(), True),
    StructField("Valor_Gastos", DoubleType(), True),
    StructField("Monto_Poliza", DoubleType(), True),
    StructField("Metodo_Pago", StringType(), True)
])

# 3. Cargar datos desde CSV
df = spark.read \
    .option("header", True) \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .option("encoding", "ISO-8859-1") \
    .schema(schema) \
    .csv("data/Ingresos-DukCar_Asesores.csv") #Archivo base

# 4. Limpieza de datos
# Corregir encoding y normalizar texto
df = df.withColumn("Nombre_Cliente", regexp_replace(col("Nombre_Cliente"), "Ãƒâ€˜", "Ñ"))

# Manejar fechas
df = df.withColumn("Fecha_Registro", to_date(col("Fecha_Registro"), "d/M/yyyy")) \
       .withColumn("Fecha_Expedicion_Poliza", to_date(col("Fecha_Expedicion_Poliza"), "d/M/yyyy"))

# Manejar valores nulos y ceros
df = df.withColumn("Monto_Poliza", when(col("Monto_Poliza") == 0, None).otherwise(col("Monto_Poliza"))) \
       .na.fill({"Valor_Prima_Neta": 0.0, "Valor_Gastos": 0.0})

# 5. Transformación de datos
# Crear columna de año-mes
df = df.withColumn("AnioMes_Registro", col("Fecha_Registro").substr(1, 7))

# Normalizar nombres de métodos de pago
df = df.withColumn("Metodo_Pago", 
                   when(col("Metodo_Pago").contains("CREDITO"), "TARJETA_CREDITO")
                   .otherwise(col("Metodo_Pago")))

# 6. Análisis Exploratorio (EDA)
# Estadísticas básicas
df.describe().show()

# Conteo por aseguradora
df.groupBy("Nombre_Aseguradora").count().orderBy("count", ascending=False).show(10, truncate=False)

# Monto total de pólizas por método de pago
df.groupBy("Metodo_Pago").sum("Monto_Poliza").show()

# Distribución temporal
df.groupBy("AnioMes_Registro").count().orderBy("AnioMes_Registro").show(10, truncate=False)

# 7. Almacenar resultados
df.write \
  .format("parquet") \
  .mode("overwrite") \
  .save("resultados_procesados/")

# Opcional: Guardar en formato legible
df.coalesce(1).write \
  .format("csv") \
  .option("header", True) \
  .mode("overwrite") \
  .save("resultados_procesados_csv/")

# 8. Detener sesión de Spark
spark.stop()