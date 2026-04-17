import requests
import argparse
import boto3
import json

# Extraction des données brutes depuis l'API Hacker news vers le bucket s3 raw

def get_top_stories(limit):
    # 1. Récupérer les IDs
    base_url = "https://hacker-news.firebaseio.com/v0"
    ids = requests.get(f"{base_url}/topstories.json").json()
    
    # 2. Initialiser le client S3 pour LocalStack
    # Note : 'localstack' est le nom du service dans ton docker-compose
    s3 = boto3.client('s3', endpoint_url='http://localstack:4566', 
                      aws_access_key_id='test', aws_secret_access_key='test')

    # 3. Récupérer le détail et stocker
    for i in range(min(limit, len(ids))):
        item_id = ids[i]
        detail = requests.get(f"{base_url}/item/{item_id}.json").json()
        
        # Sauvegarde dans le bucket 'raw'
        s3.put_object(
            Bucket='raw',
            Key=f'extract_{item_id}.json',
            Body=json.dumps(detail)
        )
        print(f"Article {item_id} stocké dans S3.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="Nombre d'articles à extraire")
    args = parser.parse_args()
    
    get_top_stories(args.limit)