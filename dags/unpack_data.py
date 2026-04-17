"""
Exercice 1 : Téléchargement et préparation des données WikiText-2.

Ce script télécharge le dataset WikiText-2 depuis HuggingFace,
puis sauvegarde chaque split dans un fichier texte séparé.

Usage:
    python src/unpack_data.py --output-dir data/raw
"""
import argparse
from pathlib import Path

from datasets import load_dataset


def download_wikitext():
    """
    Télécharge le dataset WikiText-2 depuis HuggingFace.

    Returns
    -------
    dataset : DatasetDict
        Objet contenant les splits 'train', 'validation', 'test'.
        Chaque split contient un champ 'text' avec les lignes de texte.

    Hint
    ----
    Utiliser load_dataset() avec :
        - premier argument : "Salesforce/wikitext"
        - second argument (name) : "wikitext-2-raw-v1"
    """
    # TODO: Charger et retourner le dataset
    ds = load_dataset("Salesforce/wikitext", "wikitext-2-raw-v1")
    return ds
    pass


def save_split_to_file(dataset_split, output_path: Path) -> int:
    """
    Sauvegarde un split du dataset dans un fichier texte.

    Parameters
    ----------
    dataset_split : Dataset
        Un split HuggingFace (ex: dataset["train"])
    output_path : Path
        Chemin du fichier de sortie

    Returns
    -------
    int
        Nombre de lignes non-vides écrites

    Steps
    -----
    1. Ouvrir le fichier en écriture (encoding='utf-8')
    2. Parcourir les éléments du split (champ 'text')
    3. Écrire uniquement les lignes non-vides (après strip())
    4. Retourner le compteur de lignes écrites

    Hint
    ----
    - dataset_split est itérable : for item in dataset_split
    - Chaque item est un dict avec une clé 'text'
    - Utiliser strip() pour vérifier si la ligne est vide
    """
    # TODO: Implémenter la sauvegarde
    count = 0
    for item in dataset_split:
        text = item['text'].strip()
        if text != "":
            with open(output_path, 'a', encoding='utf-8') as f:
                f.write(text + '\n')
            count += 1
    return count


def unpack_data(output_dir: str) -> None:
    """
    Télécharge WikiText-2 et sauvegarde chaque split dans un fichier.

    Parameters
    ----------
    output_dir : str
        Répertoire de sortie pour les fichiers

    Steps
    -----
    1. Créer le répertoire de sortie s'il n'existe pas
       Hint: Path(output_dir).mkdir(parents=True, exist_ok=True)

    2. Télécharger le dataset avec download_wikitext()

    3. Pour chaque split ('train', 'validation', 'test') :
       a. Construire le chemin de sortie : output_dir/wikitext-{split}.txt
       b. Appeler save_split_to_file()
       c. Afficher le nombre de lignes sauvegardées

    Expected output files
    ---------------------
    - data/raw/wikitext-train.txt
    - data/raw/wikitext-validation.txt
    - data/raw/wikitext-test.txt
    """
    output_path = Path(output_dir)

    # TODO: Implémenter la logique de téléchargement et sauvegarde
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ds = download_wikitext()
    for split in ['train', 'validation', 'test']:
        split_data = ds[split]
        output_file = output_path / f"wikitext-{split}.txt"
        count = save_split_to_file(split_data, output_file)
        print(f"Split '{split}' : {count} lignes sauvegardées dans {output_file}")
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Télécharge WikiText-2 et sauvegarde les splits en fichiers texte."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Répertoire de sortie pour les fichiers (défaut: data/raw)"
    )

    args = parser.parse_args()

    print("=== Téléchargement de WikiText-2 ===")
    unpack_data(args.output_dir)
    print("\nTerminé.")