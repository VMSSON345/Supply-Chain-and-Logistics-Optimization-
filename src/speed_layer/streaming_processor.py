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
            .option("maxOffsetsPerTrigger", 500) \
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
                approx_count_distinct("CustomerID").alias("UniqueCustomers")
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
    
    # def write_to_elasticsearch(self, streaming_df, index_name, checkpoint_path, trigger_time='1 minute'):
    #     """
    #     Ghi streaming data vào Elasticsearch
        
    #     Args:
    #         streaming_df: Streaming DataFrame
    #         index_name: Elasticsearch index name
    #         checkpoint_path: Path để lưu checkpoint
    #     """
    #     logger.info(f"Writing stream to Elasticsearch index: {index_name}")
        
    #     # Foreachbatch function để ghi vào ES
    #     def write_to_es(batch_df, batch_id):
    #         if batch_df.count() > 0:
    #             # Convert to Pandas
    #             pandas_df = batch_df.toPandas()
                
    #             # Write to Elasticsearch
    #             from elasticsearch import Elasticsearch, helpers
    #             es = Elasticsearch(['http://elasticsearch:9200'])
                
    #             actions = [
    #                 {
    #                     "_index": index_name,
    #                     "_source": row.to_dict()
    #                 }
    #                 for _, row in pandas_df.iterrows()
    #             ]
                
    #             helpers.bulk(es, actions)
    #             logger.info(f"Batch {batch_id}: Written {len(actions)} records to ES")
        
    #     # Start stream
    #     query = streaming_df.writeStream \
    #         .outputMode("update") \
    #         .foreachBatch(write_to_es) \
    #         .option("checkpointLocation", checkpoint_path) \
    #         .trigger(processingTime=trigger_time) \
    #         .start()
        
    #     return query

    def write_to_elasticsearch(self, streaming_df, index_name, checkpoint_path, doc_id_cols, trigger_time='30 seconds'):
        """
        Ghi micro-batch vào Elasticsearch bằng trình ghi gốc (không dùng toPandas).
        Hàm này thực hiện "Upsert" (Update/Insert).
        """
        logger.info(f"Writing stream to Elasticsearch index: {index_name}")

        es_options = {
            "es.nodes": "elasticsearch", # Tên service
            "es.port": "9200",
            "es.nodes.wan.only": "true",
            "es.mapping.id": "doc_id", # Yêu cầu ES dùng cột doc_id làm ID
            "es.write.operation": "upsert" # Chế độ: update nếu ID tồn tại, insert nếu không
        }

        def write_batch_to_es(batch_df, batch_id):
            """
            Hàm này được gọi cho mỗi micro-batch.
            Nó chạy trên Spark, không kéo dữ liệu về Driver.
            """
            logger.info(f"Processing Batch ID: {batch_id} for index {index_name}")

            # 1. Tạo cột 'doc_id' duy nhất
            # doc_id_cols là một danh sách, ví dụ: ["window_start", "Country"]
            batch_with_id = batch_df.withColumn("doc_id", sha1(concat_ws("||", *doc_id_cols)))

            # 2. Ghi trực tiếp sang ES
            try:
                batch_with_id.write \
                    .format("org.elasticsearch.spark.sql") \
                    .options(**es_options) \
                    .mode("append") \
                    .save(index_name)
                logger.info(f"Batch {batch_id}: Successfully upserted data to {index_name}")
            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write to {index_name}. Error: {e}")

        # Start stream
        query = streaming_df.writeStream \
            .outputMode("update") \
            .foreachBatch(write_batch_to_es) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime=trigger_time) \
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


# def run_streaming_job(spark, kafka_servers, topic, es_host, checkpoint_base):
#     """
#     Main streaming job
    
#     Args:
#         spark: SparkSession
#         kafka_servers: Kafka bootstrap servers
#         topic: Kafka topic name
#         es_host: Elasticsearch host
#         checkpoint_base: Base path for checkpoints
#     """
#     logger.info("="*60)
#     logger.info("Starting Real-time Streaming Job")
#     logger.info("="*60)
    
#     # Create processor
#     processor = RealtimeStreamProcessor(spark)
    
#     # 1. Create Kafka stream
#     stream_df = processor.create_kafka_stream(kafka_servers, topic)
    
#     # 2. Compute metrics
#     metrics = processor.compute_realtime_metrics(stream_df, window_duration="5 minutes")
    
#     # 3. Detect alerts
#     alerts = processor.detect_inventory_alerts(stream_df, threshold_multiplier=2.5)
    
#     # 4. Write streams to Elasticsearch
#     queries = []
    
#     # Revenue metrics
#     revenue_query = processor.write_to_elasticsearch(
#         metrics['revenue'],
#         'retail_realtime_revenue',
#         f"{checkpoint_base}/revenue",
#         trigger_time='1 minute'
#     )
#     queries.append(revenue_query)
    
#     # Top products
#     top_products_query = processor.write_to_elasticsearch(
#         metrics['top_products'],
#         'retail_realtime_products',
#         f"{checkpoint_base}/products",
#         trigger_time='1 minute'
#     )
#     queries.append(top_products_query)
    
#     # Alerts
#     alerts_query = processor.write_to_elasticsearch(
#         alerts,
#         'retail_inventory_alerts',
#         f"{checkpoint_base}/alerts",
#         trigger_time='1 minute'
#     )
#     queries.append(alerts_query)
    
#     logger.info(f"Started {len(queries)} streaming queries")
#     logger.info("Streaming job is running. Press Ctrl+C to stop.")
    
#     # Wait for termination
#     try:
#         spark.streams.awaitAnyTermination()
#     except KeyboardInterrupt:
#         logger.info("Stopping streaming queries...")
#         for query in queries:
#             query.stop()
    
#     logger.info("="*60)
#     logger.info("Streaming Job Stopped")
#     logger.info("="*60)
def run_streaming_job(spark, kafka_servers, topic, es_host, checkpoint_base):
    """
    Main streaming job
    """
    logger.info("="*60)
    logger.info("Starting Real-time Streaming Job")
    logger.info("="*60)

    processor = RealtimeStreamProcessor(spark)

    # 1. Create Kafka stream
    stream_df = processor.create_kafka_stream(kafka_servers, topic)

    # 2. Compute metrics
    metrics = processor.compute_realtime_metrics(stream_df, window_duration="5 minutes")

    # 3. Detect alerts
    alerts = processor.detect_inventory_alerts(stream_df, threshold_multiplier=2.5)

    # 4. Write streams to Elasticsearch (DÙNG LOGIC MỚI)
    queries = []

    # Revenue metrics
    # revenue_query = processor.write_to_elasticsearch(
    #     metrics['revenue'],
    #     'retail_realtime_revenue',
    #     f"{checkpoint_base}/revenue",
    #     doc_id_cols=["window", "Country"], # Cột để tạo ID
    #     trigger_time='30 seconds' # Giảm thời gian trigger
    # )
    # queries.append(revenue_query)

    # # Top products
    # top_products_query = processor.write_to_elasticsearch(
    #     metrics['top_products'],
    #     'retail_realtime_products',
    #     f"{checkpoint_base}/products",
    #     doc_id_cols=["window", "StockCode"], # Cột để tạo ID
    #     trigger_time='30 seconds'
    # )
    # queries.append(top_products_query)

    # # Alerts
    # alerts_query = processor.write_to_elasticsearch(
    #     alerts,
    #     'retail_inventory_alerts',
    #     f"{checkpoint_base}/alerts",
    #     doc_id_cols=["WindowStart", "StockCode", "AlertTime"], # Cột để tạo ID
    #     trigger_time='30 seconds'
    # )
    # queries.append(alerts_query)

    es_options = {
        "es.nodes": "elasticsearch", # Tên service trong docker-compose
        "es.port": "9200",
        "es.nodes.wan.only": "true", # Cần thiết khi chạy trong Docker
        "es.mapping.id": "doc_id" # Chỉ định cột ID
    }

    # Revenue metrics
    # TẠO CỘT MỚI DẠNG STRING
    revenue_df_with_id_cols = metrics['revenue'] \
        .withColumn("window_start_str", col("window.start").cast("string")) 
    
    revenue_query = processor.write_to_elasticsearch(
        revenue_df_with_id_cols, # Dùng DataFrame mới
        'retail_realtime_revenue',
        f"{checkpoint_base}/revenue",
        doc_id_cols=["window_start_str", "Country"], # Dùng cột string mới
        trigger_time='30 seconds'
    )
    queries.append(revenue_query)
    
    # Top products
    # TẠO CỘT MỚI DẠNG STRING
    top_products_df_with_id_cols = metrics['top_products'] \
        .withColumn("window_start_str", col("window.start").cast("string"))

    top_products_query = processor.write_to_elasticsearch(
        top_products_df_with_id_cols, # Dùng DataFrame mới
        'retail_realtime_products',
        f"{checkpoint_base}/products",
        doc_id_cols=["window_start_str", "StockCode"], # Dùng cột string mới
        trigger_time='30 seconds'
    )
    queries.append(top_products_query)
    
    # Alerts
    # TẠO CỘT MỚI DẠNG STRING
    alerts_df_with_id_cols = alerts \
        .withColumn("WindowStart_str", col("WindowStart").cast("string")) \
        .withColumn("AlertTime_str", col("AlertTime").cast("string"))

    alerts_query = processor.write_to_elasticsearch(
        alerts_df_with_id_cols, # Dùng DataFrame mới
        'retail_inventory_alerts',
        f"{checkpoint_base}/alerts",
        doc_id_cols=["WindowStart_str", "StockCode", "AlertTime_str"], # Dùng các cột string mới
        trigger_time='30 seconds'
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
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.7") \
        .config("spark.driver.memory", "8g") \
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
