import csv
import time
import json
import random
import logging
from datetime import datetime
from kafka import KafkaProducer

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataGenerator:
    def __init__(self, csv_file, topic_name, bootstrap_servers=['localhost:9092']):
        """
        Inicializa el generador de datos
        
        Args:
            csv_file (str): Ruta al archivo CSV
            topic_name (str): Nombre del topic de Kafka
            bootstrap_servers (list): Lista de servidores de Kafka
        """
        self.csv_file = csv_file
        self.topic_name = topic_name
        
        # Inicializar productor de Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def read_and_send(self, interval_min=0.5, interval_max=2.0):
        """
        Lee datos del CSV y los envía a Kafka en intervalos aleatorios
        
        Args:
            interval_min (float): Intervalo mínimo entre envíos (segundos)
            interval_max (float): Intervalo máximo entre envíos (segundos)
        """
        try:
            with open(self.csv_file, 'r', encoding='ISO-8859-1') as file:
                csv_reader = csv.DictReader(file)
                
                # Contador de registros enviados
                count = 0
                
                for row in csv_reader:
                    # Convertir el registro a un diccionario limpio
                    record = self._clean_record(row)
                    
                    # Enviar el registro al topic de Kafka
                    self.producer.send(self.topic_name, value=record)
                    
                    # Incrementar contador y mostrar progreso
                    count += 1
                    if count % 10 == 0:
                        logger.info(f"Enviados {count} registros al topic {self.topic_name}")
                    
                    # Esperar un intervalo aleatorio para simular llegada en tiempo real
                    time.sleep(random.uniform(interval_min, interval_max))
                
                logger.info(f"Proceso completado. Se enviaron {count} registros en total.")
                
        except Exception as e:
            logger.error(f"Error al procesar el archivo CSV: {e}")
        finally:
            self._close()
    
    def _clean_record(self, row):
        """
        Limpia y formatea un registro del CSV
        
        Args:
            row (dict): Registro leído del CSV
            
        Returns:
            dict: Registro limpio y formateado
        """
        # Procesar valores numéricos
        def parse_float(value):
            try:
                if value and value.strip():
                    return float(value)
                return 0.0
            except:
                return 0.0
        
        # Añadir timestamp para simular tiempo real
        record = {
            "timestamp": datetime.now().isoformat(),
            "ID_Ingreso": row.get("ID_Ingreso", ""),
            "Fecha_Registro": row.get("Fecha_Registro", ""),
            "Nombre_Aseguradora": row.get("Nombre_Aseguradora", ""),
            "Numero_Poliza": row.get("Numero_Poliza", ""),
            "Fecha_Expedicion_Poliza": row.get("Fecha_Expedicion_Poliza", ""),
            "Nombre_Asesor_Comercial": row.get("Nombre_Asesor_Comercial", ""),
            "Nombre_Cliente": row.get("Nombre_Cliente", ""),
            "Valor_Prima_Neta": parse_float(row.get("Valor_Prima_Neta", "0")),
            "Valor_Gastos": parse_float(row.get("Valor_Gastos", "0")),
            "Monto_Poliza": parse_float(row.get("Monto_Poliza", "0")),
            "Metodo_Pago": row.get("Metodo_Pago", "")
        }
        
        return record
    
    def _close(self):
        """Cierra el productor de Kafka"""
        if self.producer:
            self.producer.flush()
            self.producer.close()

if __name__ == "__main__":
    # Parámetros
    CSV_FILE = "data/Ingresos-DukCar_Asesores.csv"
    TOPIC_NAME = "dukcar_ingresos"
    
    # Crear y ejecutar el generador
    generator = DataGenerator(CSV_FILE, TOPIC_NAME)
    logger.info(f"Iniciando envío de datos desde {CSV_FILE} al topic {TOPIC_NAME}")
    
    # Iniciar envío de datos
    generator.read_and_send(interval_min=0.2, interval_max=1.0)
