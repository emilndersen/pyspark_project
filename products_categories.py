from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_products_with_categories(spark):
    # Переменные данные 
      # Примерные данные (как будто у нас есть таблицы)
    products = [
        (1, "Яблоко"),
        (2, "Груша"),
        (3, "Киви"),
        (4, "Арбуз"),
    ]
    categories = [
        (1, "Фрукты"),
        (2, "Экзотические"),
    ]
    product_categories = [
        (1, 1),  
        (2, 1),  
        (3, 2),  
        # Арбуз без категории
    ]

    # Создаём датафреймы
    df_products = spark.createDataFrame(products, ["product_id", "product_name"])
    df_categories = spark.createDataFrame(categories, ["category_id", "category_name"])
    df_links = spark.createDataFrame(product_categories, ["product_id", "category_id"])

    # LEFT JOIN продуктов с их категориями
    df_result = df_products.join(
        df_links, on="product_id", how="left"
    ).join(
        df_categories, on="category_id", how="left"
    ).select(
        col("product_name"), col("category_name")
    )

    return df_result
    