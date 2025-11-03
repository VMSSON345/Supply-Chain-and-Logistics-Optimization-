    """
Unit Tests for Batch Processing
"""
import unittest
import sys
sys.path.append('../src')

from pyspark.sql import SparkSession
from batch_layer.data_preprocessing import DataPreprocessor
from batch_layer.customer_segmentation import CustomerSegmentationEngine
import pandas as pd


class TestDataPreprocessing(unittest.TestCase):
    """Test data preprocessing functions"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session"""
        cls.spark = SparkSession.builder \
            .appName("TestDataPreprocessing") \
            .master("local[2]") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()
    
    def test_load_data(self):
        """Test data loading"""
        preprocessor = DataPreprocessor(self.spark)
        
        # Create sample data
        data = [
            ("536365", "85123A", "PRODUCT A", 6, "2010-12-01 08:26:00", 2.55, "17850", "United Kingdom"),
            ("536365", "71053", "PRODUCT B", 6, "2010-12-01 08:26:00", 3.39, "17850", "United Kingdom"),
        ]
        
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                  "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = self.spark.createDataFrame(data, columns)
        
        self.assertEqual(df.count(), 2)
        self.assertEqual(len(df.columns), 8)
    
    def test_clean_transactions(self):
        """Test data cleaning"""
        preprocessor = DataPreprocessor(self.spark)
        
        data = [
            ("536365", "85123A", "PRODUCT A", 6, "2010-12-01 08:26:00", 2.55, "17850", "United Kingdom"),
            ("536366", "71053", "PRODUCT B", -1, "2010-12-01 08:28:00", 3.39, "17850", "United Kingdom"),  # Negative qty
            ("536367", "84406B", "PRODUCT C", 8, "2010-12-01 08:34:00", 2.75, None, "United Kingdom"),  # Null customer
        ]
        
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                  "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = self.spark.createDataFrame(data, columns)
        df_clean = preprocessor.clean_transactions(df)
        
        # Should only keep first row
        self.assertEqual(df_clean.count(), 1)


class TestCustomerSegmentation(unittest.TestCase):
    """Test customer segmentation"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestCustomerSegmentation") \
            .master("local[2]") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_compute_rfm(self):
        """Test RFM computation"""
        engine = CustomerSegmentationEngine(self.spark)
        
        # Create sample transaction data
        data = [
            ("17850", "536365", "2010-12-01", 100.0),
            ("17850", "536366", "2010-12-05", 50.0),
            ("17851", "536367", "2010-12-10", 200.0),
        ]
        
        columns = ["CustomerID", "InvoiceNo", "InvoiceDate", "TotalAmount"]
        
        df = self.spark.createDataFrame(data, columns)
        
        # Convert InvoiceDate to timestamp
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("InvoiceDate", to_timestamp("InvoiceDate"))
        
        rfm = engine.compute_rfm(df, reference_date="2010-12-15")
        
        self.assertEqual(rfm.count(), 2)  # 2 unique customers
        self.assertTrue('Recency' in rfm.columns)
        self.assertTrue('Frequency' in rfm.columns)
        self.assertTrue('Monetary' in rfm.columns)


if __name__ == '__main__':
    unittest.main()
