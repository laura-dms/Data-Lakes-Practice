1. Étape d'Extraction (Bronze / Raw)
Source : HuggingFace (datasets)
Destination : Système de fichiers local (data/raw/)

Action : Le script télécharge le dataset WikiText-2 brut.

Format : Les données sont extraites au format texte brut (.txt) pour chaque split (train, validation, test).

But : Avoir une copie locale immuable des données sources avant tout traitement.

2. Étape de Transformation (Silver / Staging)
Source : Fichiers .txt (ou chargement direct via Python)
Destination : Base de données MySQL (Table texts)

Nettoyage : * Suppression des lignes vides (via .strip()).

Suppression des doublons (via set()).

Action : Les chaînes de caractères nettoyées sont insérées dans une base relationnelle.

Schéma MySQL : Chaque ligne possède un id, le contenu du text, et son split.

But : Structurer les données textuelles et permettre des validations SQL (compter les lignes, vérifier la qualité).

3. Étape de Chargement Final (Gold / Curated)
Source : Base MySQL
Destination : Base de données MongoDB (Collection wikitext)

Transformation complexe (NLP) : * Récupération des textes depuis MySQL.

Tokenisation via un modèle HuggingFace (ex: distilbert-base-uncased). Le texte est converti en listes de nombres (input_ids).

Enrichissement : Création de métadonnées (timestamp, nom du tokenizer, nombre de tokens).

Action : Insertion des documents JSON complexes dans MongoDB.

But : Préparer les données pour être consommées directement par un modèle de Machine Learning ou une application IA.

L'orchestrateur (Airflow) s'assure que l'étape 2 ne démarre que si l'étape 1 a réussi, et ainsi de suite.