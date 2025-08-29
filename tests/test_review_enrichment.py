import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.review_enrichment import enrich_and_count_reviews
from src.cleaning import load_and_process_data

# Charger données
products_clean, user_product_pairs, interactions_filtered, reviews = load_and_process_data()


reviews_enriched_list, word_count_list = enrich_and_count_reviews(products_clean, reviews)

# --- Affichage des résultats ---
print("=== Reviews enrichies ===")
for review in reviews_enriched_list:
    print(review)

print("\n=== Comptage des mots clés ===")
for word in word_count_list:
    print(word)
