import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.cleaning import load_and_process_data
from src.review_enrichment import enrich_and_count_reviews
from src.integrated_features import build_user_profiles

# --- Charger les données produits, interactions et reviews ---
products_clean, user_product_pairs, interactions_filtered, reviews_rdd = load_and_process_data()

# ---  enrichissement des reviews et comptage des mots ---
reviews_list, word_count_list = enrich_and_count_reviews(products_clean, reviews_rdd)

print("=== Reviews enrichies ===")
for r in reviews_list:
    print(r)

print("\n=== Comptage des mots clés ===")
for w in word_count_list:
    print(w)

# ---  construction des profils utilisateurs ---
user_profiles_list = build_user_profiles(reviews_rdd, user_product_pairs)

print("\n=== Profils utilisateurs ===")
for profile in user_profiles_list:
    print(profile)
