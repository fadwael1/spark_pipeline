import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower


def load_and_process_data():

    spark = SparkSession.builder.master("local[*]").appName("PipelineSpark").getOrCreate()
    product_catalog = spark.read.csv("data/product_catalog.csv", header=True, inferSchema=True)
    user_interaction = spark.read.csv("data/user_interaction.csv", header=True, inferSchema=True)
    reviews_df = spark.read.csv("data/user_reviews.csv", header=True, inferSchema=True)




    # Supprimer les espaces dans les noms de colonnes
    product_catalog = product_catalog.toDF(*[c.strip() for c in product_catalog.columns])
    user_interaction = user_interaction.toDF(*[c.strip() for c in user_interaction.columns])
    reviews_df = reviews_df.toDF(*[c.strip() for c in reviews_df.columns])

    # Nettoyer les valeurs string
    products_clean = product_catalog.rdd.map(lambda x: (x['product_id'].strip(), x['category'].strip().lower(), x['price']))
    interactions_filtered = user_interaction.rdd.filter(lambda x: x['action'].strip().lower() in ["click", "purchase"])
    user_product_pairs = interactions_filtered.map(lambda x: (x['user_id'].strip(), x['product_id'].strip()))
    reviews = reviews_df.rdd.map(lambda x: (x['product_id'].strip(), x['review_text'].strip(), x['user_id'].strip(), x['rating']))
    return products_clean, user_product_pairs, interactions_filtered, reviews

