import argparse
from datasets import load_dataset
import mysql.connector
from mysql.connector import Error

def download_wikitext():
    """ Charge le dataset WikiText-2 depuis HuggingFace. """
    ds = load_dataset("Salesforce/wikitext", "wikitext-2-raw-v1")
    return ds

def clean_split(dataset_split):
    """ Nettoie un split du dataset (suppression des vides et doublons). """
    text = dataset_split["text"]  
    text = [t.strip() for t in text if t.strip() != ""]  # Supprimer les lignes vides
    text = list(set(text))  # Supprimer les doublons
    return text

def create_mysql_connection(host, user, password, database, port):
    """ Crée et retourne une connexion MySQL. """
    try:
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database, port=port
        )
        return conn
    except Error as e:
        print(f"Erreur de connexion MySQL : {e}")
        return None

def create_table(connection):
    """ Crée la table 'texts' dans la base staging si elle n'existe pas. """
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS texts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            text TEXT NOT NULL,
            split VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    connection.commit()
    cursor.close()

def insert_data(connection, texts, split_name):
    """ Insère les textes nettoyés par batchs de sécurité dans la table 'texts'. """
    cursor = connection.cursor()
    query = "INSERT INTO texts (text, split) VALUES (%s, %s)"
    values = [(t, split_name) for t in texts]
    
    # Sécurité : On découpe en paquets de 5000 lignes pour éviter d'asphyxier MySQL
    batch_size = 5000
    total_inserted = 0
    
    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        cursor.executemany(query, batch)
        connection.commit()
        total_inserted += cursor.rowcount
        
    print(f"  {total_inserted} lignes insérées avec succès pour le split {split_name}.")
    cursor.close()

def validate_data(connection):
    """ Valide les données insérées en exécutant des requêtes SQL. """
    cursor = connection.cursor()
    
    # 1. Nombre de lignes par split
    query1 = "SELECT split, COUNT(*) as nb FROM texts GROUP BY split"
    cursor.execute(query1)
    results1 = cursor.fetchall()
    print("Nombre de lignes par split :")
    for split, count in results1:
        print(f"  {split}: {count}")

    # 2. Nombre de textes vides
    query2 = "SELECT COUNT(*) FROM texts WHERE TRIM(text) = ''"
    cursor.execute(query2)
    result2 = cursor.fetchone()
    print(f"Nombre de textes vides : {result2[0]}")

    # 3. Aperçu des 10 premières lignes
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
    # CORRECTION ICI : type=int pour correspondre aux attentes du connecteur MySQL
    parser.add_argument("--db-port", type=int, default=3307) 
    args = parser.parse_args()

    # 1. Charger le dataset
    print("Chargement du dataset WikiText-2...")
    dataset = download_wikitext()
    if dataset is None:
        print("Erreur lors du chargement du dataset.")
        return

    # 2. Connexion MySQL
    print(f"Connexion à MySQL sur le port {args.db_port}...")
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