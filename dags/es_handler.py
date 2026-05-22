import boto3
import json
from datetime import datetime
from elasticsearch import Elasticsearch

def transform_and_load():
    # Connexions locales
    s3 = boto3.client('s3', endpoint_url='http://localstack:4566', 
                      aws_access_key_id='test', aws_secret_access_key='test')
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])

    # 1. Récupérer les objets du bucket raw
    try:
        response = s3.list_objects_v2(Bucket='raw')
        if 'Contents' not in response:
            print("Le bucket 'raw' est vide. Lancez d'abord hn_api.py !")
            return

        for obj in response['Contents']:
            # 2. Lire le JSON
            file_content = s3.get_object(Bucket='raw', Key=obj['Key'])
            raw_data = json.loads(file_content['Body'].read().decode('utf-8'))

            # 3. Transformation (Mapping exercice 3)
            # Conversion vitale du timestamp pour Elasticsearch
            unix_time = raw_data.get("time")
            iso_date = datetime.fromtimestamp(unix_time).isoformat() if unix_time else None

            transformed_doc = {
                "id": raw_data.get("id"),
                "title": raw_data.get("title"),
                "url": raw_data.get("url"),
                "score": raw_data.get("score"),
                "timestamp": iso_date,
                "content": raw_data.get("text", "") # Le champ text devient content
            }

            # 4. Envoi vers Elasticsearch
            es.index(index="hackernews", id=transformed_doc["id"], body=transformed_doc)
            print(f"Article {transformed_doc['id']} indexé avec succès.")

    except Exception as e:
        print(f"Erreur : {e}")
        raise

if __name__ == "__main__":
    transform_and_load()