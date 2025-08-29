# main.py
import os
import logging
from pyspark.sql import SparkSession
from src.cleaning import load_and_process_data
from src.review_enrichment import enrich_and_count_reviews
from src.integrated_features import build_user_profiles
from src.user_activity import summarize_interactions

# --- Configuration des logs ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Démarrage du pipeline Spark")

# --- Initialisation Spark ---
spark = SparkSession.builder \
    .appName("PipelineSpark") \
    .master("local[*]") \
    .getOrCreate()
logging.info("SparkSession initialisé")

# --- Étape 1 : Chargement et nettoyage des données ---
products_clean, user_product_pairs, interactions_filtered, reviews_rdd = load_and_process_data()
logging.info("Données chargées et nettoyées")

# --- Sauvegarde des données nettoyées ---
clean_data_dir = "cleaned/data"
os.makedirs(clean_data_dir, exist_ok=True)

products_clean.toDF(["product_id", "category", "price"]) \
    .write.mode("overwrite").csv(os.path.join(clean_data_dir, "products_clean.csv"), header=True)
logging.info("Catalogue produit nettoyé sauvegardé")

# --- Étape 2 : Résumé des interactions ---
product_summary_list, user_summary_list = summarize_interactions(products_clean, interactions_filtered)
logging.info("Résumé des interactions généré")

with open(os.path.join(clean_data_dir, "product_summary.txt"), "w") as f:
    for line in product_summary_list:
        f.write(line + "\n")
with open(os.path.join(clean_data_dir, "user_summary.txt"), "w") as f:
    for line in user_summary_list:
        f.write(line + "\n")
logging.info("Résumé interactions sauvegardé")

# --- Étape 3 : Enrichissement des reviews ---
reviews_enriched_list, word_count_list = enrich_and_count_reviews(products_clean, reviews_rdd)
logging.info("Reviews enrichies")

with open(os.path.join(clean_data_dir, "reviews_enriched.txt"), "w") as f:
    for line in reviews_enriched_list:
        f.write(line + "\n")
with open(os.path.join(clean_data_dir, "word_count.txt"), "w") as f:
    for line in word_count_list:
        f.write(line + "\n")
logging.info("Reviews enrichies sauvegardées")

# --- Étape 4 : Construction des profils utilisateurs ---
user_profiles_list = build_user_profiles(reviews_rdd, user_product_pairs)
logging.info("Profils utilisateurs construits")

with open(os.path.join(clean_data_dir, "user_profiles.txt"), "w") as f:
    for line in user_profiles_list:
        f.write(line + "\n")
logging.info("Profils utilisateurs sauvegardés")

logging.info("Pipeline terminé avec succès")
spark.stop()
