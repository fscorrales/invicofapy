__all__ = ["db", "COLLECTIONS"]

from motor.motor_asyncio import AsyncIOMotorClient

from .__base_config import MONGODB_URI, logger

MONGO_DB_NAME = "invico"
COLLECTIONS = ["siif_rf602"]

# Inicializar la conexión con MongoDB
client = AsyncIOMotorClient(MONGODB_URI)
db = client[MONGO_DB_NAME]

# Send a ping to confirm a successful connection
try:
    client.admin.command("ping")
    logger.info("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# Función para obtener la base de datos en servicios/repositorios
def get_database():
    return db


# from pymongo.mongo_client import MongoClient
# from pymongo.server_api import ServerApi

# DB_NAME = "invico_api"
# COLLECTIONS = ["siif_rf602"]

# # Create a new client and connect to the server
# client = MongoClient(MONGODB_URI, server_api=ServerApi("1"))

# # Send a ping to confirm a successful connection
# try:
#     client.admin.command("ping")
#     logger.info("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)
