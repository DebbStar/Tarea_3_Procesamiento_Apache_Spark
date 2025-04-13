from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definir esquema para los datos de entrada
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("ID_Ingreso", StringType(), True),
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

# Función para crear directorio de resultados si no existe
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def process_batch(df, batch_id):
    """
    Procesa cada micro-batch de datos y muestra los resultados por consola
    
    Args:
        df: DataFrame del micro-batch
        batch_id: ID del batch
    """
    try:
        # Mostrar información del batch
        count = df.count()
        logger.info(f"Batch ID: {batch_id}, Registros recibidos: {count}")
        
        if count > 0:
            # Registrar el DataFrame como una vista temporal para SQL
            df.createOrReplaceTempView("ingresos")
            
            # Imprimir encabezado para separar análisis
            print("\n" + "=" * 80)
            print(f" ANÁLISIS DE DATOS EN TIEMPO REAL - BATCH {batch_id} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            
            # 1. Análisis de métodos de pago
            metodos_pago = df.groupBy("Metodo_Pago") \
                            .count() \
                            .orderBy(desc("count"))
            
            print("\n[1] DISTRIBUCIÓN DE MÉTODOS DE PAGO:")
            print("-" * 40)
            metodos_pago.show(truncate=False)
            
            # 2. Análisis de montos por aseguradora
            montos_aseguradora = df.groupBy("Nombre_Aseguradora") \
                                .agg(
                                    count("*").alias("Cantidad_Polizas"),
                                    sum("Monto_Poliza").alias("Monto_Total"),
                                    round(avg("Monto_Poliza"), 2).alias("Promedio_Monto")
                                ) \
                                .orderBy(desc("Monto_Total"))
            
            print("\n[2] MONTOS POR ASEGURADORA:")
            print("-" * 40)
            montos_aseguradora.show(truncate=False)
            
            # 3. Análisis de asesores comerciales
            asesores = df.groupBy("Nombre_Asesor_Comercial") \
                        .count() \
                        .withColumnRenamed("count", "Cantidad_Polizas") \
                        .orderBy(desc("Cantidad_Polizas"))
            
            print("\n[3] ACTIVIDAD DE ASESORES COMERCIALES:")
            print("-" * 40)
            asesores.show(truncate=False)
            
            # 4. Estadísticas de montos de pólizas
            estadisticas = df.select(
                count("Monto_Poliza").alias("Total_Polizas"),
                round(sum("Monto_Poliza"), 2).alias("Suma_Total"),
                round(avg("Monto_Poliza"), 2).alias("Promedio"),
                round(min("Monto_Poliza"), 2).alias("Minimo"),
                round(max("Monto_Poliza"), 2).alias("Maximo")
            )
            
            print("\n[4] ESTADÍSTICAS DE MONTOS DE PÓLIZAS:")
            print("-" * 40)
            estadisticas.show(truncate=False)
            
            # Imprimir pie de página
            print("\n" + "=" * 80)
            print(f" FIN DEL ANÁLISIS DEL BATCH {batch_id}")
            print("=" * 80 + "\n")
            
            # Guardar resultados como CSV para acceso posterior (opcional)
            output_dir = "resultados/streaming"
            ensure_dir(output_dir)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            
            metodos_pago.write \
                .mode("overwrite") \
                .csv(f"{output_dir}/metodos_pago_{timestamp}")
                
            montos_aseguradora.write \
                .mode("overwrite") \
                .csv(f"{output_dir}/montos_aseguradora_{timestamp}")
                
            asesores.write \
                .mode("overwrite") \
                .csv(f"{output_dir}/asesores_{timestamp}")
                
    except Exception as e:
        logger.error(f"Error al procesar batch {batch_id}: {e}")

def main():
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("DukCarStreamingKafka") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .master("local[*]") \
        .getOrCreate()
    
    # Configurar nivel de log
    spark.sparkContext.setLogLevel("WARN")
    
    # Mostrar mensaje de inicio
    print("\n" + "*" * 80)
    print("* SISTEMA DE ANÁLISIS EN TIEMPO REAL DE DATOS DE SEGUROS DUKCAR *")
    print(f"* Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} *")
    print("*" * 80 + "\n")
    print("Esperando datos de Kafka...\n")
    
    # Leer flujo de datos desde Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "dukcar_ingresos") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Convertir valor del mensaje (JSON) al esquema definido
    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Procesar cada batch
    query = parsed \
        .writeStream \
        .foreachBatch(process_batch) \
        .start()
    
    logger.info("Aplicación Spark Streaming iniciada. Esperando datos...")
    
    try:
        # Mantener la aplicación en ejecución hasta que sea detenida manualmente
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nAplicación detenida manualmente. Cerrando...")
        query.stop()
    
    spark.stop()

if __name__ == "__main__":
    main()
