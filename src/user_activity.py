def summarize_interactions(products_clean, interactions_filtered):
    # Résumé des actions par utilisateur
    user_actions = interactions_filtered.map(
        lambda x: (
            x.user_id.strip(),
            (1 if x.action.strip().lower() == 'click' else 0,
             1 if x.action.strip().lower() == 'purchase' else 0)
        )
    )

    user_summary = user_actions.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    user_summary_list = [
        f"{user} : {clicks} click, {purchases} achat"
        for user, (clicks, purchases) in user_summary.collect()
    ]

    # Nombre d'interactions par produit
    product_interactions = interactions_filtered.map(
        lambda x: (x.product_id.strip(), 1)
    ).reduceByKey(lambda a, b: a + b)

    # Infos produit
    product_info = products_clean.map(lambda x: (x[0], (x[1], x[2])))  # (product_id, (category, price))

    # Jointure interactions + infos produit
    product_enriched = product_interactions.join(product_info)
    
    product_enriched_list = [
        f"{product} : {interactions} interaction, catégorie = {category}, prix = {price}"
        for product, (interactions, (category, price)) in product_enriched.collect()
    ]


    return product_enriched_list, user_summary_list
