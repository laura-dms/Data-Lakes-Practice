"""
Exercice 2 : Chargement de WikiText-2, nettoyage, insertion dans MySQL (Staging).

Usage:
    python src/load_to_staging.py \
        --db-host localhost --db-user root \
        --db-password root --db-name staging

Architecture du Pipeline 

HuggingFace (datasets)--> Nettoyage (Python)--> MySQL (Staging)--> Tokenisation (transformers)--> MongoDB (Curated)
        
"""


import argparse
from datasets import load_dataset
import mysql.connector
from mysql.connector import Error
from datasets import load_dataset


def download_wikitext():
    """
    Charge le dataset WikiText-2 depuis HuggingFace.
    
    Retourne l'objet dataset contenant les splits 'train', 'validation', 'test'.
    Chaque élément possède un champ 'text'.
    """
    # ds contient les splits : ds["train"], ds["validation"], ds["test"] 
    # # Chaque ´el´ement a un seul champ : "text"
    
    # TODO: Charger le dataset avec load_dataset()
    #   - Nom du dataset : "Salesforce/wikitext"
    #   - Configuration : "wikitext-2-raw-v1"
    #   - Retourner l'objet dataset
    ds = load_dataset("Salesforce/wikitext", "wikitext-2-raw-v1")
    return ds


def clean_split(dataset_split):
    """
    Nettoie un split du dataset.
    
    Args:
        dataset_split: un split HuggingFace (ex: dataset["train"])
    
    Returns:
        Liste de chaînes de caractères nettoyées.
    """
    # TODO:
    #   1. Extraire la liste des textes depuis dataset_split (champ "text")
    #   2. Supprimer les lignes vides ou ne contenant que des espaces
    #      Indice : utiliser str.strip() pour vérifier
    #   3. Supprimer les doublons
    #      Indice : convertir en set ou utiliser pandas.DataFrame.drop_duplicates()
    #   4. Retourner la liste nettoyée

    text = dataset_split["text"]  # Liste de textes
    text = [t.strip() for t in text if t.strip() != ""]  # Supprimer les lignes vides
    text = list(set(text))  # Supprimer les doublons
    return text


def create_mysql_connection(host, user, password, database, port):
    """
    Crée et retourne une connexion MySQL.
    Retourne None en cas d'erreur.
    """
    # TODO: Utiliser mysql.connector.connect() avec les paramètres fournis
    #   - Gérer l'exception mysql.connector.Error
    #   - Afficher un message d'erreur si la connexion échoue
    #   - Retourner la connexion ou None
    try:
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database, port=port
        )
        return conn
    except Error as e:
        print(f"Erreur de connexion MySQL : {e}")
        return None


def create_table(connection):
    """
    Crée la table 'texts' dans la base staging si elle n'existe pas.
    
    Schéma attendu :
        id          INT AUTO_INCREMENT PRIMARY KEY
        text        TEXT NOT NULL
        split       VARCHAR(20) NOT NULL
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """
    # TODO:
    #   1. Créer un curseur
    cursor = connection.cursor()
    #   2. Exécuter la requête CREATE TABLE IF NOT EXISTS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS texts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            text TEXT NOT NULL,
            split VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    #   3. Commit
    connection.commit()
    cursor.close()


def insert_data(connection, texts, split_name):
    """
    Insère les textes nettoyés dans la table 'texts'.
    
    Args:
        connection: connexion MySQL active
        texts: liste de chaînes de caractères
        split_name: nom du split ("train", "validation", "test")
    """
    # TODO:
    #   1. Créer un curseur
    #   2. Préparer la requête INSERT INTO texts (text, split) VALUES (%s, %s)
    #   3. Préparer les valeurs : liste de tuples (text, split_name)
    #   4. Utiliser cursor.executemany() pour l'insertion par batch
    #   5. Commit et afficher le nombre de lignes insérées

    cursor = connection.cursor()
    query = "INSERT INTO texts (text, split) VALUES (%s, %s)"
    values = [(t, split_name) for t in texts]
    cursor.executemany(query, values)
    connection.commit()
    print(f"  {cursor.rowcount} lignes insérées.")
    cursor.close()


def validate_data(connection):
    """
    Valide les données insérées en exécutant des requêtes SQL.
    
    Requêtes à exécuter :
        1. Nombre de lignes par split (GROUP BY)
        2. Nombre de textes vides (WHERE TRIM(text) = '')
        3. Aperçu des 5 premières lignes (id, début du texte, split)
    """
    # TODO: Exécuter chaque requête, récupérer les résultats avec fetchall(),
    #   et les afficher de manière lisible.

    cursor = connection.cursor()
    query1 = "SELECT split, COUNT(*) as nb FROM texts GROUP BY split"
    cursor.execute(query1)
    results1 = cursor.fetchall()
    print("Nombre de lignes par split :")
    for split, count in results1:
        print(f"  {split}: {count}")

    query2 = "SELECT COUNT(*) FROM texts WHERE TRIM(text) = ''"
    cursor.execute(query2)
    result2 = cursor.fetchone()
    print(f"Nombre de textes vides : {result2[0]}")

    query3 = "SELECT id, LEFT(text, 50) as preview, split FROM texts LIMIT 10"
    cursor.execute(query3)
    results3 = cursor.fetchall()
    print("Aperçu des 10 premières lignes :")
    for id, preview, split in results3:
        print(f"  ID: {id}, Split: {split}, Text Preview: '{preview}'")
    cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Charge WikiText-2 dans MySQL (zone Staging)"
    )
    parser.add_argument("--db-host", type=str, default="localhost")
    parser.add_argument("--db-user", type=str, default="root")
    parser.add_argument("--db-password", type=str, default="root")
    parser.add_argument("--db-name", type=str, default="staging")
    parser.add_argument("--db-port", type=str, default="3307")
    args = parser.parse_args()

    # 1. Charger le dataset
    print("Chargement du dataset WikiText-2...")
    dataset = download_wikitext()
    if dataset is None:
        print("Erreur lors du chargement du dataset.")
        return

    # 2. Connexion MySQL
    print("Connexion à MySQL...")
    connection = create_mysql_connection(
        args.db_host, args.db_user, args.db_password, args.db_name, args.db_port
    )
    if connection is None:
        return

    # 3. Créer la table
    print("Création de la table...")
    create_table(connection)

    # 4. Nettoyer et insérer chaque split
    for split_name in ["train", "validation", "test"]:
        print(f"\nTraitement du split '{split_name}'...")
        cleaned = clean_split(dataset[split_name])
        print(f"  {len(cleaned)} lignes après nettoyage")
        insert_data(connection, cleaned, split_name)

    # 5. Validation
    print("\n--- Validation des données ---")
    validate_data(connection)

    # 6. Fermeture
    connection.close()
    print("\nTerminé.")


if __name__ == "__main__":
    main()