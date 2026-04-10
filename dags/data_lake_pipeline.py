# Impl´ementez le pipeline Airflow pour effectuer les tˆaches suivantes :
# • Extraction : T´el´echargez les donn´ees depuis Hugging Face et stockez-les dans le
# bucket raw.
# • Transformation : Nettoyez les donn´ees et ins´erez-les dans une base MySQL.
# • Chargement : R´ecup´erez les donn´ees nettoy´ees, tokenisez-les, et ins´erez-les dans
# une collection MongoDB.

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from transformers import AutoTokenizer

# Importez des fonctions des scripts du TP3
import unpack_data
import load_to_staging
import staging_to_curated 

def data_to_raw(**kwargs):
    unpack_data(output_dir="data/raw")

def raw_to_curated(**kwargs):
    """
    Lit les données (via HuggingFace ou local), les nettoie et les insère dans MySQL.
    """
    # 1. Paramètres de connexion (à adapter selon votre environnement)
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "staging",
        "port": "3307"
    }

    # 2. Chargement du dataset
    print("Étape 1 : Chargement de WikiText-2...")
    dataset = load_to_staging.download_wikitext()
    
    # 3. Connexion à MySQL
    print("Étape 2 : Connexion à la base MySQL Staging...")
    connection = load_to_staging.create_mysql_connection(
        db_config["host"], 
        db_config["user"], 
        db_config["password"], 
        db_config["database"], 
        db_config["port"]
    )

    if connection:
        try:
            # 4. Création de la table si elle n'existe pas
            load_to_staging.create_table(connection)

            # 5. Boucle de nettoyage et insertion
            for split_name in ["train", "validation", "test"]:
                print(f"Traitement du split : {split_name}")
                
                # Appel de votre fonction de nettoyage
                cleaned_data = load_to_staging.clean_split(dataset[split_name])
                
                # Appel de votre fonction d'insertion
                load_to_staging.insert_data(connection, cleaned_data, split_name)
            
            # 6. Validation finale dans les logs Airflow
            load_to_staging.validate_data(connection)
            
        finally:
            connection.close()
            print("Connexion MySQL fermée.")
    else:
        raise Exception("Impossible de se connecter à MySQL. Vérifiez vos paramètres.")


def curated_to_staging(**kwargs):
    """
    Récupère les données de MySQL, les tokenise et les charge dans MongoDB.
    """
    # 1. Configuration des paramètres
    config = {
        "mysql_host": "localhost",
        "mysql_user": "root",
        "mysql_password": "root",
        "mysql_db": "staging",
        "mysql_port": "3307",
        "mongo_uri": "mongodb://localhost:27017/",
        "tokenizer_name": "distilbert-base-uncased",
        "max_length": 512
    }

    # 2. Récupération des données MySQL
    # On peut boucler sur les splits ou n'en traiter qu'un selon votre besoin
    for split in ["train", "validation", "test"]:
        print(f"--- Traitement du split {split} pour MongoDB ---")
        
        rows = staging_to_curated.get_staging_data(
            config["mysql_host"],
            config["mysql_user"],
            config["mysql_password"],
            config["mysql_db"],
            config["mysql_port"],
            split=split
        )

        if not rows:
            print(f"Aucune donnée trouvée pour le split {split}. Passage au suivant.")
            continue

        # 3. Initialisation du Tokenizer et Tokenisation
        # Note : On initialise ici pour plus de clarté, 
        # mais en production on le ferait hors de la boucle.
        tokenizer = AutoTokenizer.from_pretrained(config["tokenizer_name"])
        texts = [row[1] for row in rows]
        
        all_tokens = staging_to_curated.tokenize_texts(
            texts, 
            tokenizer, 
            config["max_length"]
        )

        # 4. Préparation des dictionnaires (Documents)
        documents = staging_to_curated.prepare_documents(
            rows, 
            all_tokens, 
            split, 
            config["tokenizer_name"], 
            config["max_length"]
        )

        # 5. Insertion dans MongoDB
        staging_to_curated.insert_to_mongodb(documents, config["mongo_uri"])

    # 6. Vérification finale
    staging_to_curated.verify_mongodb(config["mongo_uri"])
    

# D´efinition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
with DAG(
    'data_lake_pipeline', 
    default_args=default_args, 
    description='Pipeline ETL pour le traitement des données',
    schedule=timedelta(days=1),
    schedule_interval=None,  # Déclenchement manuel
    catchup=False            # Évite de lancer les exécutions passées depuis 2024
) as dag:

    # 1. Extraction : HuggingFace -> Fichiers locaux (.txt)
    extract_task = PythonOperator(
        task_id='extract_data_to_raw',
        python_callable=data_to_raw
    )

    # 2. Transformation : Nettoyage -> MySQL (Staging)
    transform_task = PythonOperator(
        task_id='transform_data_to_mysql',
        python_callable=raw_to_curated
    )

    # 3. Chargement : Tokenisation -> MongoDB (Curated)
    load_task = PythonOperator(
        task_id='load_data_to_mongodb',
        python_callable=curated_to_staging
    )

    # Définition des dépendances (Workflow)
    extract_task >> transform_task >> load_task