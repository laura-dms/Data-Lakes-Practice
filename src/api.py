from fastapi import FastAPI, status, HTTPException, Query
from datetime import datetime
import os
import boto3
import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient

app = FastAPI(title="Data Lake API Health Check")

# Configuration dynamique des hôtes (Local local vs Docker interne)
LOCALSTACK_HOST = os.getenv("LOCALSTACK_HOST", "localhost")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
# Le port MySQL change si on l'appelle depuis l'extérieur (3307) ou l'intérieur de Docker (3306)
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3307))

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    health_status = {
        "api_status": "online",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "connections_status": {
            "s3": "unhealthy",
            "mysql": "unhealthy",
            "mongodb": "unhealthy"
        }
    }
    
    # 1. Vérification de LocalStack S3
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=f'http://{LOCALSTACK_HOST}:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        # On liste les buckets pour valider que la connexion fonctionne
        s3.list_buckets()
        health_status["connections_status"]["s3"] = "healthy"
    except Exception as e:
        print(f"S3 Health Check Failed: {e}")

    # 2. Vérification de MySQL
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user="root",
            password="root",
            database="staging",
            port=MYSQL_PORT,
            connect_timeout=3  # Évite de bloquer l'API si la DB est down
        )
        if connection.is_connected():
            health_status["connections_status"]["mysql"] = "healthy"
            connection.close()
    except Exception as e:
        print(f"MySQL Health Check Failed: {e}")

    # 3. Vérification de MongoDB
    try:
        # serverSelectionTimeoutMS évite de bloquer l'API trop longtemps en cas de panne
        mongo_uri = f"mongodb://{MONGODB_HOST}:27017/"
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        
        # On force une commande d'administration (ping) pour valider la connexion réelle
        mongo_client.admin.command('ping')
        health_status["connections_status"]["mongodb"] = "healthy"
        mongo_client.close()
    except Exception as e:
        print(f"MongoDB Health Check Failed: {e}")

    return 
    
from fastapi import FastAPI, status, HTTPException, Query
from datetime import datetime
import os
import boto3
import mysql.connector
from pymongo import MongoClient

app = FastAPI(title="Data Lake API")

# Configuration dynamique des hôtes
LOCALSTACK_HOST = os.getenv("LOCALSTACK_HOST", "localhost")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3307))

# Initialisation globale du client S3 pour l'API
s3_client = boto3.client(
    's3',
    endpoint_url=f'http://{LOCALSTACK_HOST}:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    health_status = {
        "api_status": "online",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "connections_status": {
            "s3": "unhealthy",
            "mysql": "unhealthy",
            "mongodb": "unhealthy"
        }
    }
    
    # 1. Vérification de LocalStack S3
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=f'http://{LOCALSTACK_HOST}:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        # On liste les buckets pour valider que la connexion fonctionne
        s3.list_buckets()
        health_status["connections_status"]["s3"] = "healthy"
    except Exception as e:
        print(f"S3 Health Check Failed: {e}")

    # 2. Vérification de MySQL
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user="root",
            password="root",
            database="staging",
            port=MYSQL_PORT,
            connect_timeout=3  # Évite de bloquer l'API si la DB est down
        )
        if connection.is_connected():
            health_status["connections_status"]["mysql"] = "healthy"
            connection.close()
    except Exception as e:
        print(f"MySQL Health Check Failed: {e}")

    # 3. Vérification de MongoDB
    try:
        # serverSelectionTimeoutMS évite de bloquer l'API trop longtemps en cas de panne
        mongo_uri = f"mongodb://{MONGODB_HOST}:27017/"
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        
        # On force une commande d'administration (ping) pour valider la connexion réelle
        mongo_client.admin.command('ping')
        health_status["connections_status"]["mongodb"] = "healthy"
        mongo_client.close()
    except Exception as e:
        print(f"MongoDB Health Check Failed: {e}")

    return health_status


# ==========================================
# EXERCICE 2 : Endpoint pour la Couche Raw
# ==========================================
@app.get("/raw", status_code=status.HTTP_200_OK)
def get_raw_data(limit: int = Query(default=10, ge=1, description="Nombre maximum d'éléments à retourner")):
    """
    Récupère les données brutes depuis le bucket S3 'raw', 
    les convertit en JSON et permet de limiter le nombre de résultats.
    """
    bucket_name = "raw"
    raw_data_list = []

    try:
        # 1. Lister les fichiers présents dans le bucket 'raw'
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        # Si le bucket est vide ou n'existe pas encore d'objets
        if 'Contents' not in response:
            return {"bucket": bucket_name, "count": 0, "data": []}

        # 2. Parcourir les fichiers (en respectant la limite demandée)
        files_to_process = response['Contents'][:limit]

        for file_obj in files_to_process:
            file_key = file_obj['Key']

            # 3. Récupérer le contenu du fichier textuel depuis S3
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = s3_object['Body'].read().decode('utf-8')

            # 4. Structurer la donnée sous forme de dictionnaire (JSON-ready)
            raw_data_list.append({
                "filename": file_key,
                "extracted_at": file_obj['LastModified'].isoformat(),
                "size_bytes": file_obj['Size'],
                "content": file_content.strip()
            })

        return {
            "bucket": bucket_name,
            "limit_applied": limit,
            "count": len(raw_data_list),
            "data": raw_data_list
        }

    except s3_client.exceptions.NoSuchBucket:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Le bucket '{bucket_name}' n'existe pas. Lancez d'abord votre pipeline Airflow."
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de la récupération des données S3 : {str(e)}"
        )


def get_mysql_db_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user="root",
            password="root",
            database="staging",
            port=MYSQL_PORT,
            connect_timeout=3
        )
        return conn
    except Error as e:
        print(f"Erreur lors de la connexion à MySQL : {e}")
        return None

# ==========================================
# EXERCICE 3 : Endpoint pour la Couche Staging (MySQL)
# ==========================================
@app.get("/staging", status_code=status.HTTP_200_OK)
def get_staging_data(
    split: str = Query(default=None, description="Filtrer par split (train, validation, test)"),
    limit: int = Query(default=10, ge=1, le=100, description="Nombre max de lignes à retourner (max 100)")
):
    """
    Récupère les données transformées et stockées dans la table MySQL 'texts'.
    Permet de filtrer par type de split et de limiter le nombre de résultats retournés.
    """
    connection = get_mysql_db_connection()
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Impossible de se connecter à la base de données MySQL Staging."
        )
    
    try:
        cursor = connection.cursor(dictionary=True) # dictionary=True permet de récupérer les résultats sous forme de dict
        
        # Construction dynamique de la requête SQL selon la présence ou non du filtre 'split'
        if split:
            # Sécurité élémentaire : validation de la valeur du split
            if split not in ["train", "validation", "test"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Split invalide. Choisissez parmi : train, validation, test."
                )
            query = "SELECT id, text, split, created_at FROM texts WHERE split = %s LIMIT %s"
            params = (split, limit)
        else:
            query = "SELECT id, text, split, created_at FROM texts LIMIT %s"
            params = (limit,)
            
        cursor.execute(query, params)
        records = cursor.fetchall()
        
        # Convertir les objets datetime en chaînes ISO pour la sérialisation JSON
        for row in records:
            if isinstance(row["created_at"], datetime):
                row["created_at"] = row["created_at"].isoformat()

        return {
            "database": "staging",
            "table": "texts",
            "filter_split": split,
            "limit_applied": limit,
            "count": len(records),
            "data": records
        }

    except Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de l'exécution de la requête SQL : {str(e)}"
        )
    finally:
        cursor.close()
        connection.close()

# ==========================================
# EXERCICE 4 : Endpoint pour la Couche Curated (MongoDB)
# ==========================================

# Fonction utilitaire pour obtenir le client MongoDB (Exercice 4)
def get_mongodb_client():
    try:
        # On définit un timeout rapide de 3 secondes pour ne pas figer l'API
        mongo_uri = f"mongodb://{MONGODB_HOST}:27017/"
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        return client
    except Exception as e:
        print(f"Erreur MongoDB : {e}")
        return None
@app.get("/curated", status_code=status.HTTP_200_OK)
def get_curated_data(
    split: str = Query(default=None, description="Filtrer par split (train, validation, test)"),
    limit: int = Query(default=10, ge=1, le=100, description="Nombre max de documents à retourner (max 100)")
):
    """
    Récupère les documents tokenisés stockés dans MongoDB (Base: curated, Collection: wikitext).
    """
    mongo_client = get_mongodb_client()
    if not mongo_client:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Impossible d'initialiser le client MongoDB."
        )

    try:
        # CORRECTION 1 : Alignement sur le script (Base: curated, Collection: wikitext)
        db = mongo_client["curated"]
        collection = db["wikitext"]

        # Construction dynamique du filtre
        query_filter = {}
        if split:
            if split not in ["train", "validation", "test"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Split invalide. Choisissez parmi : train, validation, test."
                )
            # CORRECTION 2 : On cible "metadata.split" car le champ est imbriqué
            query_filter["metadata.split"] = split

        # Récupération des données
        cursor = collection.find(query_filter, {"_id": 0}).limit(limit)
        documents = list(cursor)

        return {
            "database": "curated",
            "collection": "wikitext",
            "filter_split": split,
            "limit_applied": limit,
            "count": len(documents),
            "data": documents
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de la récupération des données MongoDB : {str(e)}"
        )
    finally:
        mongo_client.close()