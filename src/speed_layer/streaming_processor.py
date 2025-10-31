"""
Real-time Stream Processing with Spark Structured Streaming
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealtimeStreamProcessor:
    """Xử lý luồng dữ liệu real-time từ Kafka"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_kafka_stream(self, kafka_servers, topic):
        """
        Tạo streaming DataFrame từ Kafka
        
        Args:
            kafka_servers: Kafka bootstrap servers
            topic: Tên topic để đọc
            
        Returns:
            Streaming DataFrame
        """
        logger.info(f"Creating Kafka stream for topic: {topic}")
        
        # Define schema cho transaction
        schema = StructType([
            StructField("InvoiceNo", StringType()),
            StructField("StockCode", StringType()),
            StructField("Description", StringType()),
            StructField("Quantity", IntegerType()),
            StructField("InvoiceDate", StringType()),
            StructField("UnitPrice", DoubleType()),
            StructField("CustomerID", StringType()),
            StructField("Country", StringType()),
            StructField("TotalAmount", DoubleType()),
            StructField("EventTime", StringType())
        ])
        
        # Read from Kafka
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp
        parsed_df = parsed_df.withColumn(
            "EventTime",
            to_timestamp("EventTime")
        )
        
        return parsed_df
    
    def compute_realtime_metrics(self, stream_df, window_duration="5 minutes"):
        """
        Tính toán các metrics real-time với window aggregation
        
        Args:
            stream_df: Streaming DataFrame
            window_duration: Độ dài cửa sổ thời gian
            
        Returns:
            Multiple streaming DataFrames cho các metrics khác nhau
        """
        logger.info(f"Computing real-time metrics with window: {window_duration}")
        
        # 1. Total Revenue per minute
        revenue_per_window = stream_df \
            .withWatermark("EventTime", "10 minutes") \
            .groupBy(
                window("EventTime", window_duration, "1 minute"),
                "Country"
            ).agg(
                sum("TotalAmount").alias("TotalRevenue"),
                count("*").alias("TransactionCount"),
                countDistinct("CustomerID").alias("UniqueCustomers")
            )
        
        # 2. Top products in last N minutes
        top_products = stream_df \
            .withWatermark("EventTime", "10 minutes") \
            .groupBy(
                window("EventTime", "10 minutes", "2 minutes"),
                "StockCode",
                "Description"
            ).agg(
                sum("Quantity").alias("TotalQuantity"),
                sum("TotalAmount").alias("TotalRevenue"),
                count("*").alias("TransactionCount")
            )
        
        # 3. Customer activity (for alerts)
        customer_activity = stream_df \
            .withWatermark("EventTime", "10 minutes") \
            .groupBy(
                window("EventTime", "5 minutes"),
                "CustomerID"
            ).agg(
                sum("TotalAmount").alias("CustomerSpend"),
                count("*").alias("TransactionCount")
            )
        
        return {
            'revenue': revenue_per_window,
            'top_products': top_products,
            'customer_activity': customer_activity
        }
    
    def detect_inventory_alerts(self, stream_df, threshold_multiplier=3.0):
        """
        Phát hiện các alert về tồn kho (sản phẩm bán đột biến)
        
        Args:
            stream_df: Streaming DataFrame
            threshold_multiplier: Ngưỡng để trigger alert
            
        Returns:
            Streaming DataFrame chứa alerts
        """
        logger.info("Setting up inventory alerts...")
        
        # Tính trung bình và phát hiện anomaly
        product_stats = stream_df \
            .withWatermark("EventTime", "15 minutes") \
            .groupBy(
                window("EventTime", "15 minutes", "5 minutes"),
                "StockCode",
                "Description"
            ).agg(
                sum("Quantity").alias("TotalQuantity"),
                avg("Quantity").alias("AvgQuantity"),
                stddev("Quantity").alias("StdQuantity")
            )
        
        # Alert khi vượt ngưỡng
        alerts = product_stats.filter(
            col("TotalQuantity") > col("AvgQuantity") * threshold_multiplier
        ).select(
            col("window.start").alias("WindowStart"),
            col("window.end").alias("WindowEnd"),
            "StockCode",
            "Description",
            "TotalQuantity",
            "AvgQuantity",
            lit("HIGH_DEMAND_ALERT").alias("AlertType"),
            current_timestamp().alias("AlertTime")
        )
        
        return alerts
    
    def write_to_elasticsearch(self, streaming_df, index_name, checkpoint_path):
        """
        Ghi streaming data vào Elasticsearch
        
        Args:
            streaming_df: Streaming DataFrame
            index_name: Elasticsearch index name
            checkpoint_path: Path để lưu checkpoint
        """
        logger.info(f"Writing stream to Elasticsearch index: {index_name}")
        
        # Foreachbatch function để ghi vào ES
        def write_to_es(batch_df, batch_id):
            if batch_df.count() > 0:
                # Convert to Pandas
                pandas_df = batch_df.toPandas()
                
                # Write to Elasticsearch
                from elasticsearch import Elasticsearch, helpers
                es = Elasticsearch(['http://elasticsearch:9200'])
                
                actions = [
                    {
                        "_index": index_name,
                        "_source": row.to_dict()
                    }
                    for _, row in pandas_df.iterrows()
                ]
                
                helpers.bulk(es, actions)
                logger.info(f"Batch {batch_id}: Written {len(actions)} records to ES")
        
        # Start stream
        query = streaming_df.writeStream \
            .outputMode("update") \
            .foreachBatch(write_to_es) \
            .option("checkpointLocation", checkpoint_path) \
            .start()
        
        return query
    
    def write_to_console(self, streaming_df, output_mode="update"):
        """Debug: Ghi ra console"""
        query = streaming_df.writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        return query


def run_streaming_job(spark, kafka_servers, topic, es_host, checkpoint_base):
    """
    Main streaming job
    
    Args:
        spark: SparkSession
        kafka_servers: Kafka bootstrap servers
        topic: Kafka topic name
        es_host: Elasticsearch host
        checkpoint_base: Base path for checkpoints
    """
    logger.info("="*60)
    logger.info("Starting Real-time Streaming Job")
    logger.info("="*60)
    
    # Create processor
    processor = RealtimeStreamProcessor(spark)
    
    # 1. Create Kafka stream
    stream_df = processor.create_kafka_stream(kafka_servers, topic)
    
    # 2. Compute metrics
    metrics = processor.compute_realtime_metrics(stream_df, window_duration="5 minutes")
    
    # 3. Detect alerts
    alerts = processor.detect_inventory_alerts(stream_df, threshold_multiplier=2.5)
    
    # 4. Write streams to Elasticsearch
    queries = []
    
    # Revenue metrics
    revenue_query = processor.write_to_elasticsearch(
        metrics['revenue'],
        'retail_realtime_revenue',
        f"{checkpoint_base}/revenue"
    )
    queries.append(revenue_query)
    
    # Top products
    top_products_query = processor.write_to_elasticsearch(
        metrics['top_products'],
        'retail_realtime_products',
        f"{checkpoint_base}/products"
    )
    queries.append(top_products_query)
    
    # Alerts
    alerts_query = processor.write_to_elasticsearch(
        alerts,
        'retail_inventory_alerts',
        f"{checkpoint_base}/alerts"
    )
    queries.append(alerts_query)
    
    logger.info(f"Started {len(queries)} streaming queries")
    logger.info("Streaming job is running. Press Ctrl+C to stop.")
    
    # Wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming queries...")
        for query in queries:
            query.stop()
    
    logger.info("="*60)
    logger.info("Streaming Job Stopped")
    logger.info("="*60)


if __name__ == "__main__":
    # Initialize Spark với Kafka packages
    spark = SparkSession.builder \
        .appName("RetailRealtimeStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    # Configuration
    KAFKA_SERVERS = "kafka:29092"
    TOPIC = "retail_transactions"
    ES_HOST = "http://elasticsearch:9200"
    CHECKPOINT_BASE = "/opt/spark-data/checkpoints"
    
    # Run streaming job
    run_streaming_job(spark, KAFKA_SERVERS, TOPIC, ES_HOST, CHECKPOINT_BASE)
    
    spark.stop()
