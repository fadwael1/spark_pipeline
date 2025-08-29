
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.cleaning import load_and_process_data

products_clean, user_product_pairs, interactions_filtered, reviews= load_and_process_data()
print("Products clean:")
print(products_clean.collect())
print("User-product pairs:")
print(user_product_pairs.collect())
print("Interactions filtered:")
print(interactions_filtered .collect())
print("reviews")
print(reviews .collect())
