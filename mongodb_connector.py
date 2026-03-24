"""
Módulo de conexión e integración con MongoDB
Para guardar datos extraídos de Excel en MongoDB
"""

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGODB_URI = "mongodb+srv://cristian0402218_db_user:Hz3AdKNoX9Bc3hs2@cluster0.ukqqx1n.mongodb.net/?appName=Cluster0"
DB_NAME = "vacunacion_db"
COLLECTION_VACUNAS = "vacunas"
COLLECTION_REGISTROS = "registros_extraccion"


class MongoDBConnector:
    """Clase para manejar la conexión y operaciones con MongoDB"""

    def __init__(self):
        """Inicializa la conexión a MongoDB"""
        self.client = None
        self.db = None
        self.conectar()

    def conectar(self):
        """Establece conexión con MongoDB"""
        try:
            self.client = MongoClient(MONGODB_URI, server_api=ServerApi('1'))
            self.client.admin.command('ping')
            self.db = self.client[DB_NAME]
            logger.info("✓ Conexión exitosa a MongoDB")
            return True
        except Exception as e:
            logger.error(f"✗ Error al conectar a MongoDB: {e}")
            return False

    def desconectar(self):
        """Cierra la conexión con MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Desconectado de MongoDB")

    def guardar_datos_vacunas(self, df: pd.DataFrame, fuente: str = "Excel", tamanyo_chunk: int = 10000) -> dict:
        """
        Guarda datos de vacunación en MongoDB (optimizado para archivos grandes)

        Args:
            df (pd.DataFrame): DataFrame con los datos a guardar
            fuente (str): Origen de los datos
            tamanyo_chunk (int): Tamaño de chunks para inserción

        Returns:
            dict: Resultado de la operación con estadísticas
        """
        try:
            collection = self.db[COLLECTION_VACUNAS]
            total_registros = len(df)
            total_insertados = 0
            todas_ids = []

            for inicio in range(0, total_registros, tamanyo_chunk):
                fin = min(inicio + tamanyo_chunk, total_registros)
                chunk = df.iloc[inicio:fin]

                datos = chunk.to_dict('records')

                for registro in datos:
                    registro['fecha_insercion'] = datetime.now()
                    registro['fuente'] = fuente
                    registro['procesado'] = False

                resultado = collection.insert_many(datos)
                total_insertados += len(resultado.inserted_ids)
                todas_ids.extend(resultado.inserted_ids)

                if total_registros > 50000:
                    porcentaje = (fin / total_registros) * 100
                    logger.info(f"  Progreso: {porcentaje:.1f}% ({fin}/{total_registros})")

            stats = {
                'exito': True,
                'registros_insertados': total_insertados,
                'ids_insertados': todas_ids,
                'fecha': datetime.now(),
                'tamanyo_original': total_registros,
                'procesado_en_chunks': total_registros > 50000
            }

            logger.info(f"✓ {stats['registros_insertados']} registros guardados en MongoDB")
            return stats

        except Exception as e:
            logger.error(f"✗ Error al guardar datos: {e}")
            return {
                'exito': False,
                'error': str(e),
                'fecha': datetime.now()
            }

    def obtener_datos_vacunas(self, filtro: dict = None) -> list:
        """Obtiene datos de vacunación de MongoDB"""
        try:
            collection = self.db[COLLECTION_VACUNAS]
            if filtro is None:
                filtro = {}

            datos = list(collection.find(filtro))
            logger.info(f"✓ {len(datos)} registros recuperados de MongoDB")
            return datos

        except Exception as e:
            logger.error(f"✗ Error al obtener datos: {e}")
            return []

    def registrar_extraccion(self, archivo: str, estadisticas: dict) -> dict:
        """Registra cada extracción de datos realizada"""
        try:
            collection = self.db[COLLECTION_REGISTROS]

            registro = {
                'archivo': archivo,
                'fecha_extraccion': datetime.now(),
                'estadisticas': estadisticas,
                'estado': 'completada'
            }

            resultado = collection.insert_one(registro)

            logger.info(f"✓ Extracción registrada: {archivo}")
            return {
                'exito': True,
                'id_registro': resultado.inserted_id
            }

        except Exception as e:
            logger.error(f"✗ Error al registrar extracción: {e}")
            return {
                'exito': False,
                'error': str(e)
            }

    def obtener_estadisticas(self) -> dict:
        """Obtiene estadísticas generales de la base de datos"""
        try:
            collection_vacunas = self.db[COLLECTION_VACUNAS]
            collection_registros = self.db[COLLECTION_REGISTROS]

            total_vacunas = collection_vacunas.count_documents({})
            vacunas_procesadas = collection_vacunas.count_documents({'procesado': True})
            total_extracciones = collection_registros.count_documents({})

            return {
                'total_registros_vacunas': total_vacunas,
                'registros_procesados': vacunas_procesadas,
                'registros_pendientes': total_vacunas - vacunas_procesadas,
                'total_extracciones': total_extracciones,
                'fecha_consulta': datetime.now()
            }

        except Exception as e:
            logger.error(f"✗ Error al obtener estadísticas: {e}")
            return {'error': str(e)}
