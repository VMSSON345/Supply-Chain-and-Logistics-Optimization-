"""
Real-time Metrics Computation
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealtimeMetricsCalculator:
    """Tính toán các metrics real-time"""
    
    def __init__(self):
        self.metrics = {}
    
    def compute_revenue_metrics(self, stream_df, window_duration="5 minutes"):
        """
        Tính toán revenue metrics theo window
        
        Returns:
            - Total revenue per window
            - Transaction count
            - Unique customers
            - Average order value
        """
        logger.info("Computing revenue metrics...")
        
        revenue_metrics = stream_df \
            .withWatermark("EventTime", "10 minutes") \
            .groupBy(
                window("EventTime", window_duration, "1 minute"),
                "Country"
            ).agg(
                sum("TotalAmount").alias("TotalRevenue"),
                count("*").alias("TransactionCount"),
                countDistinct("CustomerID").alias("UniqueCustomers"),
                avg("TotalAmount").alias("AvgOrderValue"),
                sum("Quantity").alias("TotalQuantity")
            ) \
            .withColumn("RevenuePerCustomer", 
                       col("TotalRevenue") / col("UniqueCustomers"))
        
        return revenue_metrics
    
    def compute_product_metrics(self, stream_df, window_duration="10 minutes"):
        """
        Tính toán product metrics
        
        Returns:
            - Top selling products
            - Revenue per product
            - Quantity sold per product
        """
        logger.info("Computing product metrics...")
        
        product_metrics = stream_df \
            .withWatermark("EventTime", "15 minutes") \
            .groupBy(
                window("EventTime", window_duration, "2 minutes"),
                "StockCode",
                "Description"
            ).agg(
                sum("Quantity").alias("TotalQuantity"),
                sum("TotalAmount").alias("TotalRevenue"),
                count("*").alias("TransactionCount"),
                avg("UnitPrice").alias("AvgPrice"),
                countDistinct("CustomerID").alias("UniqueCustomers")
            ) \
            .withColumn("RevenuePerTransaction",
                       col("TotalRevenue") / col("TransactionCount"))
        
        return product_metrics
    
    def compute_customer_metrics(self, stream_df, window_duration="15 minutes"):
        """
        Tính toán customer behavior metrics
        """
        logger.info("Computing customer metrics...")
        
        customer_metrics = stream_df \
            .withWatermark("EventTime", "20 minutes") \
            .groupBy(
                window("EventTime", window_duration, "5 minutes"),
                "CustomerID",
                "Country"
            ).agg(
                sum("TotalAmount").alias("CustomerSpend"),
                count("*").alias("TransactionCount"),
                sum("Quantity").alias("TotalItems"),
                countDistinct("StockCode").alias("UniqueProducts"),
                avg("TotalAmount").alias("AvgTransactionValue")
            )
        
        return customer_metrics
    
    def compute_hourly_trends(self, stream_df):
        """
        Tính toán trends theo giờ
        """
        logger.info("Computing hourly trends...")
        
        hourly_trends = stream_df \
            .withColumn("Hour", hour("EventTime")) \
            .groupBy(
                window("EventTime", "1 hour"),
                "Hour"
            ).agg(
                sum("TotalAmount").alias("HourlyRevenue"),
                count("*").alias("HourlyTransactions"),
                countDistinct("CustomerID").alias("HourlyCustomers")
            )
        
        return hourly_trends
    
    def compute_country_metrics(self, stream_df, window_duration="10 minutes"):
        """
        Metrics theo quốc gia
        """
        logger.info("Computing country metrics...")
        
        country_metrics = stream_df \
            .withWatermark("EventTime", "15 minutes") \
            .groupBy(
                window("EventTime", window_duration),
                "Country"
            ).agg(
                sum("TotalAmount").alias("CountryRevenue"),
                count("*").alias("CountryTransactions"),
                countDistinct("CustomerID").alias("CountryCustomers"),
                countDistinct("StockCode").alias("UniqueProducts"),
                avg("TotalAmount").alias("AvgOrderValue")
            ) \
            .withColumn("RevenuePerCustomer",
                       col("CountryRevenue") / col("CountryCustomers"))
        
        return country_metrics


def create_all_metrics(stream_df):
    """
    Tạo tất cả metrics
    """
    calculator = RealtimeMetricsCalculator()
    
    metrics = {
        'revenue': calculator.compute_revenue_metrics(stream_df, "5 minutes"),
        'products': calculator.compute_product_metrics(stream_df, "10 minutes"),
        'customers': calculator.compute_customer_metrics(stream_df, "15 minutes"),
        'hourly': calculator.compute_hourly_trends(stream_df),
        'countries': calculator.compute_country_metrics(stream_df, "10 minutes")
    }
    
    return metrics
