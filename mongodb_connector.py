"""
Módulo de conexión e integración con MongoDB
Para guardar datos extraídos de Excel en MongoDB
Colecciones: datos_internos, datos_externos, datos_no_estructurados
"""

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import pandas as pd
import logging
import certifi

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGODB_URI = "mongodb+srv://cristian0402218_db_user:Hz3AdKNoX9Bc3hs2@cluster0.ukqqx1n.mongodb.net/?appName=Cluster0"
DB_NAME = "vacunacion_db"
COLLECTION_INTERNOS = "datos_internos"
COLLECTION_EXTERNOS = "datos_externos"
COLLECTION_NO_ESTRUCTURADOS = "datos_no_estructurados"
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
            self.client = MongoClient(MONGODB_URI, server_api=ServerApi('1'), tls=True, tlsAllowInvalidCertificates=True)
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

    def guardar_datos_internos(self, df: pd.DataFrame, columnas: list, fuente: str = "Excel", tamanyo_chunk: int = 10000) -> dict:
        """
        Guarda datos internos (estructurados) en MongoDB

        Args:
            df: DataFrame con todos los datos
            columnas: lista de columnas estructuradas a guardar
            fuente: origen de los datos
            tamanyo_chunk: tamaño de chunks para inserción
        """
        try:
            collection = self.db[COLLECTION_INTERNOS]

            cols_guardar = [c for c in columnas if c in df.columns]
            if "_hoja" in df.columns:
                cols_guardar.append("_hoja")
            df_internos = df[cols_guardar]

            total_registros = len(df_internos)
            total_insertados = 0

            for inicio in range(0, total_registros, tamanyo_chunk):
                fin = min(inicio + tamanyo_chunk, total_registros)
                chunk = df_internos.iloc[inicio:fin]

                datos = chunk.to_dict('records')
                for registro in datos:
                    registro['fecha_insercion'] = datetime.now()
                    registro['fuente'] = fuente
                    registro['tipo'] = 'interno'

                resultado = collection.insert_many(datos)
                total_insertados += len(resultado.inserted_ids)

                if total_registros > 50000:
                    porcentaje = (fin / total_registros) * 100
                    logger.info(f"  Internos progreso: {porcentaje:.1f}% ({fin}/{total_registros})")

            logger.info(f"✓ {total_insertados} registros internos guardados en MongoDB")
            return {'exito': True, 'registros_insertados': total_insertados, 'fecha': datetime.now()}

        except Exception as e:
            logger.error(f"✗ Error al guardar datos internos: {e}")
            return {'exito': False, 'error': str(e), 'fecha': datetime.now()}

    def guardar_datos_no_estructurados(self, df: pd.DataFrame, columnas: list, fuente: str = "Excel") -> dict:
        """
        Guarda datos no estructurados (texto libre/comentarios del Excel) en MongoDB

        Args:
            df: DataFrame con todos los datos
            columnas: lista de columnas no estructuradas (texto largo)
            fuente: origen de los datos
        """
        try:
            collection = self.db[COLLECTION_NO_ESTRUCTURADOS]
            registros = []

            for _, fila in df.iterrows():
                for col in columnas:
                    valor = fila[col]
                    if pd.notna(valor) and str(valor).strip():
                        registros.append({
                            'columna': col,
                            'contenido': str(valor),
                            'hoja': fila.get('_hoja', 'N/A'),
                            'fecha_insercion': datetime.now(),
                            'fuente': fuente,
                            'tipo': 'no_estructurado'
                        })

            total_insertados = 0
            if registros:
                # Insertar en chunks de 5000
                for inicio in range(0, len(registros), 5000):
                    fin = min(inicio + 5000, len(registros))
                    resultado = collection.insert_many(registros[inicio:fin])
                    total_insertados += len(resultado.inserted_ids)

            logger.info(f"✓ {total_insertados} registros no estructurados guardados en MongoDB")
            return {'exito': True, 'registros_insertados': total_insertados, 'fecha': datetime.now()}

        except Exception as e:
            logger.error(f"✗ Error al guardar datos no estructurados: {e}")
            return {'exito': False, 'error': str(e), 'fecha': datetime.now()}

    def guardar_datos_externos(self, archivos: dict, fuente: str = "archivo_externo") -> dict:
        """
        Guarda datos externos (archivos TXT/JSON/PDF) en MongoDB

        Args:
            archivos: dict {nombre_archivo: contenido}
            fuente: origen de los datos
        """
        try:
            collection = self.db[COLLECTION_EXTERNOS]
            registros = []

            for nombre, contenido in archivos.items():
                ext = nombre.rsplit('.', 1)[-1].lower() if '.' in nombre else 'desconocido'
                registros.append({
                    'nombre_archivo': nombre,
                    'contenido': contenido,
                    'formato': ext,
                    'tamanyo_chars': len(contenido),
                    'fecha_insercion': datetime.now(),
                    'fuente': fuente,
                    'tipo': 'externo'
                })

            total_insertados = 0
            if registros:
                resultado = collection.insert_many(registros)
                total_insertados = len(resultado.inserted_ids)

            logger.info(f"✓ {total_insertados} archivos externos guardados en MongoDB")
            return {'exito': True, 'registros_insertados': total_insertados, 'fecha': datetime.now()}

        except Exception as e:
            logger.error(f"✗ Error al guardar datos externos: {e}")
            return {'exito': False, 'error': str(e), 'fecha': datetime.now()}

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
            col_internos = self.db[COLLECTION_INTERNOS]
            col_externos = self.db[COLLECTION_EXTERNOS]
            col_no_estruct = self.db[COLLECTION_NO_ESTRUCTURADOS]
            col_registros = self.db[COLLECTION_REGISTROS]

            return {
                'datos_internos': col_internos.count_documents({}),
                'datos_externos': col_externos.count_documents({}),
                'datos_no_estructurados': col_no_estruct.count_documents({}),
                'total_extracciones': col_registros.count_documents({}),
                'fecha_consulta': datetime.now()
            }

        except Exception as e:
            logger.error(f"✗ Error al obtener estadísticas: {e}")
            return {'error': str(e)}
