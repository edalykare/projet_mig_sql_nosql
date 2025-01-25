import pandas as pd
import psycopg2
from psycopg2 import sql
import numpy as np

# Configuration de la connexion à la base de données
db_params = {
    "dbname": "DBR",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

def create_table():
    # Création de la table avec les colonnes appropriées
    create_table_query = """
    CREATE TABLE IF NOT EXISTS formations (
        session INTEGER,
        etablissement_id VARCHAR(20),
        etablissement_nom TEXT,
        etablissement_type TEXT,
        formation_type TEXT,
        formation_nom_long TEXT,
        mentions_specialites TEXT,
        formation_apprentissage TEXT,
        internat TEXT,
        amenagement TEXT,
        informations_complementaires TEXT,
        region VARCHAR(100),
        departement VARCHAR(100),
        commune VARCHAR(100),
        lien_fiche TEXT,
        lien_statistiques TEXT,
        site_internet TEXT,
        localisation TEXT,
        formation_nom_court TEXT,
        code_formation_parcoursup VARCHAR(50),
        code_portail_parcoursup VARCHAR(50),
        etablissement_id_paysage VARCHAR(50),
        composante_id_paysage VARCHAR(50),
        rnd VARCHAR(50),
        code_formation VARCHAR(50)
    )
    """
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        print("Table créée avec succès")
    except Exception as e:
        print(f"Erreur lors de la création de la table: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def import_csv(file_path):
    try:
        # Lecture du fichier CSV avec pandas
        # na_values permet de spécifier les valeurs à considérer comme NULL
        df = pd.read_csv(file_path, 
                        sep=';', 
                        encoding='utf-8',
                        na_values=['', 'NA', 'N/A', 'null', 'NULL', 'None'])
        
        # Remplacement des NaN de pandas par None pour PostgreSQL
        df = df.replace({np.nan: None})
        
        # Connexion à la base de données
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Compteurs pour le suivi
        total_rows = 0
        successful_rows = 0
        
        # Préparation des données pour l'insertion
        for _, row in df.iterrows():
            try:
                total_rows += 1
                insert_query = """
                INSERT INTO formations (
                    session, etablissement_id, etablissement_nom, etablissement_type,
                    formation_type, formation_nom_long, mentions_specialites,
                    formation_apprentissage, internat, amenagement,
                    informations_complementaires, region, departement, commune,
                    lien_fiche, lien_statistiques, site_internet, localisation,
                    formation_nom_court, code_formation_parcoursup,
                    code_portail_parcoursup, etablissement_id_paysage,
                    composante_id_paysage, rnd, code_formation
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = tuple(row)
                cur.execute(insert_query, values)
                successful_rows += 1
                
            except Exception as e:
                print(f"Erreur lors de l'insertion de la ligne {total_rows}: {e}")
                continue
        
        # Validation des changements
        conn.commit()
        print(f"Import terminé: {successful_rows}/{total_rows} lignes importées avec succès")
        
    except Exception as e:
        print(f"Erreur lors de l'importation: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Créer la table
    create_table()
    
    # Importer les données
    csv_file_path = "data/cartographie_formations_parcoursup.csv"  # Remplacez par le chemin de votre fichier CSV
    import_csv(csv_file_path)