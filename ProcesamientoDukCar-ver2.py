from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

# 1. Inicializar sesión de Spark
spark = SparkSession.builder \
    .appName("ProcesamientoDukCar") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Esquema personalizado para optimizar la lectura
custom_schema = StructType([
    StructField("ID_Ingreso", IntegerType(), nullable=False),
    StructField("Fecha_Registro", StringType(), nullable=True),
    StructField("Nombre_Aseguradora", StringType(), nullable=True),
    StructField("Numero_Poliza", StringType(), nullable=True),
    StructField("Fecha_Expedicion_Poliza", StringType(), nullable=True),
    StructField("Nombre_Asesor_Comercial", StringType(), nullable=True),
    StructField("Nombre_Cliente", StringType(), nullable=True),
    StructField("Valor_Prima_Neta", DoubleType(), nullable=True),
    StructField("Valor_Gastos", DoubleType(), nullable=True),
    StructField("Monto_Poliza", DoubleType(), nullable=True),
    StructField("Metodo_Pago", StringType(), nullable=True)
])

# 3. Cargar datos con manejo de caracteres especiales
df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("encoding", "ISO-8859-1") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .schema(custom_schema) \
    .csv("data/Ingresos-DukCar_Asesores.csv")  # Archivo base

# 4. Limpieza avanzada de datos
def clean_text(column):
    return F.regexp_replace(column, r"[^\x00-\x7FñÑáéíóúÁÉÍÓÚ]", "")

df = df.withColumn("Nombre_Cliente", clean_text(col("Nombre_Cliente"))) \
       .withColumn("Nombre_Aseguradora", clean_text(col("Nombre_Aseguradora")))

# 5. Transformación de fechas y tipos
date_format = "dd/MM/yyyy"
df = df.withColumn("Fecha_Registro", to_date(col("Fecha_Registro"), date_format)) \
       .withColumn("Fecha_Expedicion_Poliza", to_date(col("Fecha_Expedicion_Poliza"), date_format)) \
       .withColumn("Monto_Poliza", round(col("Monto_Poliza"), 2))

# 6. Manejo de valores nulos/ceros
df = df.fillna({
    "Valor_Prima_Neta": 0.0,
    "Valor_Gastos": 0.0,
    "Metodo_Pago": "NO_ESPECIFICADO"
}).replace(0.0, None, subset=["Monto_Poliza"])

# 7. Feature Engineering
df = df.withColumn("Dias_Entre_Expedicion_Registro", 
                  datediff(col("Fecha_Registro"), col("Fecha_Expedicion_Poliza"))) \
       .withColumn("Trimestre", quarter(col("Fecha_Registro")))

# 8. Análisis Exploratorio (EDA) Avanzado
# Análisis temporal
temporal_analysis = df.groupBy(year("Fecha_Registro").alias("Año"),
                              month("Fecha_Registro").alias("Mes")) \
                    .agg(
                        count("*").alias("Total_Polizas"),
                        sum("Monto_Poliza").alias("Monto_Total"),
                        avg("Monto_Poliza").alias("Promedio_Monto")
                    ).orderBy("Año", "Mes")

# Top 10 clientes
top_clientes = df.groupBy("Nombre_Cliente") \
               .agg(sum("Monto_Poliza").alias("Monto_Total")) \
               .orderBy(desc("Monto_Total")) \
               .limit(10)

# Distribución de métodos de pago
pago_distribucion = df.groupBy("Metodo_Pago") \
                    .agg(
                        count("*").alias("Cantidad"),
                        sum("Monto_Poliza").alias("Monto_Total")
                    ).withColumn("Porcentaje", 
                               round(col("Cantidad")/df.count()*100, 2))

# 9. Persistencia de resultados
df.write \
  .format("parquet") \
  .mode("overwrite") \
  .partitionBy("Trimestre") \
  .save("resultados/procesado_completo/")

temporal_analysis.write \
  .format("csv") \
  .option("header", "true") \
  .mode("overwrite") \
  .save("resultados/analisis_temporal/")

# 10. Optimización para lectura futura
df.createOrReplaceTempView("datos_polizas")
spark.sql("CACHE TABLE datos_polizas")

# 11. Cierre de sesión
spark.stop()