import pandas as pd
import numpy as np
from pymongo import MongoClient
import time
from datetime import timedelta

# Configuration de la connexion MongoDB
mongo_params = {
    "host": "localhost",
    "port": 27017,
    "username": "root",
    "password": "password"
}

def connect_mongodb():
    try:
        # Construction de l'URI de connexion
        client = MongoClient(
            host=mongo_params["host"],
            port=mongo_params["port"],
            username=mongo_params["username"],
            password=mongo_params["password"]
        )
        # Sélection de la base de données
        db = client["formations_mongodb"]
        return client, db
    except Exception as e:
        print(f"Erreur de connexion à MongoDB: {e}")
        return None, None

def import_csv(file_path):
    start_time = time.time()
    client = None
    
    try:
        # Connexion à MongoDB
        client, db = connect_mongodb()
        if not client:
            return
        
        # Sélection de la collection
        collection = db["formations_mongodb"]
        
        print("Début de la lecture du fichier CSV...")
        # Lecture du fichier CSV avec pandas
        df = pd.read_csv(file_path, 
                        sep=';', 
                        encoding='utf-8',
                        na_values=['', 'NA', 'N/A', 'null', 'NULL', 'None'])
        
        # Remplacement des NaN par None
        df = df.replace({np.nan: None})
        print(f"Lecture du fichier terminée. {len(df)} lignes trouvées.")
        
        # Compteurs pour le suivi
        total_rows = 0
        successful_rows = 0
        
        print("Début de l'importation des données...")
        # Conversion des données en documents MongoDB
        for _, row in df.iterrows():
            try:
                total_rows += 1
                # Conversion de la ligne en dictionnaire
                document = {
                    "session": row["Session"],
                    "etablissement": {
                        "id": row["Identifiant de l'établissement"],
                        "nom": row["Nom de l'établissement"],
                        "type": row["Types d'établissement"],
                        "id_paysage": row["etablissement_id_paysage"],
                        "site_internet": row["Site internet de l'établissement"]
                    },
                    "formation": {
                        "type": row["Types de formation"],
                        "nom_long": row["Nom long de la formation"],
                        "nom_court": row["Nom court de la formation"],
                        "mentions_specialites": row["Mentions/Spécialités"],
                        "apprentissage": row["Formations en apprentissage"],
                        "code_formation": row["code_formation"],
                        "code_formation_parcoursup": row["Code interne Parcoursup de la formation"],
                        "code_portail_parcoursup": row["Code interne Parcoursup pour les portails"]
                    },
                    "caracteristiques": {
                        "internat": row["Internat"],
                        "amenagement": row["Aménagement"],
                        "informations_complementaires": row["Informations complémentaires"]
                    },
                    "localisation": {
                        "region": row["Région"],
                        "departement": row["Département"],
                        "commune": row["Commune"],
                        "coordonnees": row["Localisation"]
                    },
                    "liens": {
                        "fiche": row["Lien vers la fiche formation"],
                        "statistiques": row["Lien vers les données statistiques pour l'année antérieure"]
                    },
                    "metadata": {
                        "composante_id_paysage": row["composante_id_paysage"],
                        "rnd": row["rnd"]
                    }
                }
                
                # Insertion du document
                collection.insert_one(document)
                successful_rows += 1
                
                # Affichage de la progression tous les 1000 enregistrements
                if successful_rows % 1000 == 0:
                    print(f"Progression: {successful_rows}/{total_rows} lignes traitées")
                
            except Exception as e:
                print(f"Erreur lors de l'insertion de la ligne {total_rows}: {e}")
                continue
        
        # Calcul de la durée totale
        end_time = time.time()
        duration = end_time - start_time
        duration_formatted = str(timedelta(seconds=int(duration)))
        
        # Affichage du résumé
        print("\nRésumé de l'importation:")
        print(f"Durée totale: {duration_formatted}")
        print(f"Nombre total de lignes traitées: {total_rows}")
        print(f"Nombre de lignes importées avec succès: {successful_rows}")
        print(f"Nombre de lignes en erreur: {total_rows - successful_rows}")
        if total_rows > 0:
            print(f"Taux de réussite: {(successful_rows/total_rows)*100:.2f}%")
        
    except Exception as e:
        print(f"Erreur lors de l'importation: {e}")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    # Importer les données
    csv_file_path = "data/cartographie_formations_parcoursup.csv"  # Remplacez par le chemin de votre fichier CSV
    import_csv(csv_file_path)