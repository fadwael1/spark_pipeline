def build_user_profiles(reviews_rdd, user_product_pairs):
    
    # --- S'assurer que la rating est bien float ---
    def parse_review(r):
        try:
            product_id = r[0]
            review_text = r[1]
            user_id = r[2]
            rating = float(r[3])  # conversion en float
            review_text = review_text.strip('"') if review_text else ""
            return (user_id, (product_id, rating, review_text))
        except Exception as e:
            print(f"Erreur parse review {r}: {e}")
            return None

    # Appliquer la fonction et filtrer les erreurs
    reviews_parsed = reviews_rdd.map(parse_review).filter(lambda x: x is not None)

    # --- Grouper les reviews par utilisateur ---
    user_reviews_grouped = reviews_parsed.groupByKey().mapValues(list)

    # --- Calculer la note moyenne par utilisateur ---
    user_avg_rating = user_reviews_grouped.mapValues(
        lambda revs: sum([r[1] for r in revs]) / len(revs)
    )

    # --- Fonction pour extraire les 3 mots les plus fréquents ---
    def top_words(revs):
        words = []
        for r in revs:
            if r[2]:
                words += r[2].lower().replace(",", "").split()
        word_counts = {}
        for w in words:
            if w.strip():
                word_counts[w] = word_counts.get(w, 0) + 1
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        return [w[0] for w in sorted_words[:3]]

    user_top_words = user_reviews_grouped.mapValues(top_words)

    # --- Produits vus/achetés par utilisateur ---
    user_products = user_product_pairs.groupByKey().mapValues(lambda x: list(set(x)))

    # --- Combiner toutes les informations ---
    user_profiles = user_products.join(user_avg_rating).join(user_top_words) \
        .map(lambda x: (x[0], {
            "products": x[1][0][0],
            "avg_rating": x[1][0][1],
            "top_words": x[1][1]
        }))

    # --- Liste lisible ---
    user_profiles_list = [
        f"{user} : produits = {profile['products']}, "
        f"note moyenne = {round(profile['avg_rating'],2)}, "
        f"top mots = {profile['top_words']}"
        for user, profile in user_profiles.collect()
    ]

    return user_profiles_list
