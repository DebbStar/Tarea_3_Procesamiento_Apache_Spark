from kafka.admin import KafkaAdminClient, NewTopic
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """
    Crea un topic en Kafka si no existe
    
    Args:
        topic_name (str): Nombre del topic
        num_partitions (int): Número de particiones
        replication_factor (int): Factor de replicación
    """
    try:
        # Crear cliente administrador
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='kafka-admin'
        )
        
        # Verificar si el topic ya existe
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logger.info(f"El topic '{topic_name}' ya existe.")
            return
        
        # Crear nuevo topic
        topic = NewTopic(name=topic_name, 
                         num_partitions=num_partitions,
                         replication_factor=replication_factor)
        
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' creado exitosamente.")
        
    except Exception as e:
        logger.error(f"Error al crear el topic: {e}")
    finally:
        try:
            admin_client.close()
        except:
            pass

if __name__ == "__main__":
    # Parámetros del topic
    TOPIC_NAME = "dukcar_ingresos"
    NUM_PARTITIONS = 3  # Múltiples particiones para paralelismo
    REPLICATION_FACTOR = 1  # En un entorno de desarrollo, usar 1
    
    # Crear el topic
    create_topic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR)
