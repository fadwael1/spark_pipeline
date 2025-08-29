def enrich_and_count_reviews(products_clean, reviews):
    # --- Préparer infos produit ---
    product_info = products_clean.map(lambda x: (x[0], (x[1], x[2])))  # (product_id, (category, price))

    # --- Préparer reviews ---
    reviews_kv = reviews.map(lambda x: (x[0], (x[1].strip('"'), x[2], x[3])))
    

    # --- Jointure reviews + infos produit ---
    reviews_enriched = reviews_kv.join(product_info)  

    # --- Stocker liste lisible des reviews enrichies ---
    reviews_enriched_list = [
        f"{review[0]} : '{review[1][0][0]}' (user {review[1][0][1]}), catégorie = {review[1][1][0]}, prix = {review[1][1][1]}"
        for review in reviews_enriched.collect()
    ]
    # --- Tokenisation des mots ---
    words = reviews_enriched.flatMap(lambda x: x[1][0][0].lower().replace(",", "").replace('"', '').split(" "))

    # --- Comptage des mots ---
    word_pairs = words.map(lambda w: (w, 1))
    word_count = word_pairs.reduceByKey(lambda a, b: a + b)

    # --- Liste lisible des mots et fréquence ---
    word_count_list = [f"{word} : {count}" for word, count in word_count.collect()]

    return reviews_enriched_list, word_count_list
