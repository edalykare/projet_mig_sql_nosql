import pandas as pd
import numpy as np
import redis
import time
from datetime import timedelta

# Configuration de la connexion Redis
redis_params = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}

def connect_redis():
    try:
        # Connexion à Redis
        client = redis.Redis(
            host=redis_params["host"],
            port=redis_params["port"],
            db=redis_params["db"]
        )
        # Vérification de la connexion
        client.ping()
        print("Connexion à Redis réussie.")
        return client
    except Exception as e:
        print(f"Erreur de connexion à Redis: {e}")
        return None

def convert_none_to_empty(value):
    """Convertit les valeurs None en chaîne vide."""
    return "" if value is None else value

def import_csv(file_path):
    start_time = time.time()
    client = None
    
    try:
        # Connexion à Redis
        client = connect_redis()
        if not client:
            return
        
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
        # Conversion des données en clés-valeurs Redis
        for _, row in df.iterrows():
            try:
                total_rows += 1
                # Création d'une clé unique pour chaque enregistrement
                key = f"formation:{row['Session']}:{row['Identifiant de l\'établissement']}"
                
                # Création d'un dictionnaire pour stocker les valeurs
                document = {
                    "session": convert_none_to_empty(row["Session"]),
                    "etablissement_id": convert_none_to_empty(row["Identifiant de l'établissement"]),
                    "etablissement_nom": convert_none_to_empty(row["Nom de l'établissement"]),
                    "etablissement_type": convert_none_to_empty(row["Types d'établissement"]),
                    "etablissement_id_paysage": convert_none_to_empty(row["etablissement_id_paysage"]),
                    "etablissement_site_internet": convert_none_to_empty(row["Site internet de l'établissement"]),
                    "formation_type": convert_none_to_empty(row["Types de formation"]),
                    "formation_nom_long": convert_none_to_empty(row["Nom long de la formation"]),
                    "formation_nom_court": convert_none_to_empty(row["Nom court de la formation"]),
                    "formation_mentions_specialites": convert_none_to_empty(row["Mentions/Spécialités"]),
                    "formation_apprentissage": convert_none_to_empty(row["Formations en apprentissage"]),
                    "formation_code": convert_none_to_empty(row["code_formation"]),
                    "formation_code_parcoursup": convert_none_to_empty(row["Code interne Parcoursup de la formation"]),
                    "formation_code_portail_parcoursup": convert_none_to_empty(row["Code interne Parcoursup pour les portails"]),
                    "caracteristiques_internat": convert_none_to_empty(row["Internat"]),
                    "caracteristiques_amenagement": convert_none_to_empty(row["Aménagement"]),
                    "caracteristiques_informations_complementaires": convert_none_to_empty(row["Informations complémentaires"]),
                    "localisation_region": convert_none_to_empty(row["Région"]),
                    "localisation_departement": convert_none_to_empty(row["Département"]),
                    "localisation_commune": convert_none_to_empty(row["Commune"]),
                    "localisation_coordonnees": convert_none_to_empty(row["Localisation"]),
                    "liens_fiche": convert_none_to_empty(row["Lien vers la fiche formation"]),
                    "liens_statistiques": convert_none_to_empty(row["Lien vers les données statistiques pour l'année antérieure"]),
                    "metadata_composante_id_paysage": convert_none_to_empty(row["composante_id_paysage"]),
                    "metadata_rnd": convert_none_to_empty(row["rnd"])
                }
                
                # Insertion des données dans Redis
                client.hset(key, mapping=document)
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