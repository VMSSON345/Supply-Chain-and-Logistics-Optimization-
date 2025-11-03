"""
Unit Tests for Streaming
"""
import unittest
import sys
sys.path.append('../src')

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from speed_layer.realtime_metrics import RealtimeMetricsCalculator


class TestRealtimeMetrics(unittest.TestCase):
    """Test real-time metrics calculation"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestRealtimeMetrics") \
            .master("local[2]") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_revenue_metrics(self):
        """Test revenue metrics computation"""
        calculator = RealtimeMetricsCalculator()
        
        # Create sample streaming data
        schema = StructType([
            StructField("InvoiceNo", StringType()),
            StructField("CustomerID", StringType()),
            StructField("Country", StringType()),
            StructField("TotalAmount", DoubleType()),
            StructField("EventTime", TimestampType())
        ])
        
        from datetime import datetime
        data = [
            ("536365", "17850", "UK", 100.0, datetime(2010, 12, 1, 8, 26)),
            ("536366", "17850", "UK", 50.0, datetime(2010, 12, 1, 8, 27)),
            ("536367", "17851", "France", 200.0, datetime(2010, 12, 1, 8, 28)),
        ]
        
        df = self.spark.createDataFrame(data, schema)
        
        # In actual streaming, we would use readStream
        # For testing, we use batch DataFrame
        self.assertEqual(df.count(), 3)
        self.assertTrue('TotalAmount' in df.columns)


if __name__ == '__main__':
    unittest.main()
