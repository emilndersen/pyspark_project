import unittest
from pyspark.sql import SparkSession
from products_categories import get_products_with_categories

class TestProductsCategories(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestApp").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_products_with_categories(self):
        df_result = get_products_with_categories(self.spark)
        rows = df_result.collect()

        # Преобразуем в словарь для удобства
        result = {row['product_name']: row['category_name'] for row in rows}

        self.assertEqual(result["Яблоко"], "Фрукты")
        self.assertEqual(result["Груша"], "Фрукты")
        self.assertEqual(result["Киви"], "Экзотические")
        self.assertIsNone(result["Арбуз"])  # без категории

if __name__ == '__main__':
    unittest.main()