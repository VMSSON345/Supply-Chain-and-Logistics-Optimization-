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
        """
        logger.info(f"Creating Kafka stream for topic: {topic}")
        
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
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 500) \
            .load()
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        parsed_df = parsed_df.withColumn(
            "EventTime",
            to_timestamp("EventTime")
        )
        
        return parsed_df
    
    def compute_realtime_metrics(self, stream_df, window_duration="5 minutes"):
        """
        Tính toán các metrics real-time với window aggregation
        """
        logger.info(f"Computing real-time metrics with window: {window_duration}")
        
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
        """
        logger.info("Setting up inventory alerts...")
        
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
            batch_with_id = batch_df.withColumn("doc_id", sha1(concat_ws("||", *doc_id_cols)))

            # 2. Ghi trực tiếp sang ES
            try:
                # Ghi DataFrame (batch_with_id) vào Elasticsearch
                batch_with_id.write \
                    .format("org.elasticsearch.spark.sql") \
                    .options(**es_options) \
                    .mode("append") \
                    .save(index_name) # Ghi vào index được chỉ định
                    
                logger.info(f"Batch {batch_id}: Successfully upserted {batch_with_id.count()} records to {index_name}")
            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write to {index_name}. Error: {e}")
                raise e # Ném lỗi để dừng stream

        # Start stream
        query = streaming_df.writeStream \
            .outputMode("update") \
            .foreachBatch(write_batch_to_es) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime=trigger_time) \
            .start()

        return query
    

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

    # 4. Write streams to Elasticsearch (LOGIC MỚI ĐÃ SỬA LỖI)
    queries = []

    # Revenue metrics
    # TẠO CỘT MỚI DẠNG STRING TỪ 'window.start'
    # revenue_df_for_es = metrics['revenue'] \
    #     .withColumn("window_start_str", col("window.start").cast("string")) 
    
    # revenue_query = processor.write_to_elasticsearch(
    #     revenue_df_for_es, # Dùng DataFrame mới
    #     'retail_realtime_revenue',
    #     f"{checkpoint_base}/revenue",
    #     doc_id_cols=["window_start_str", "Country"], # Dùng cột string mới
    #     trigger_time='30 seconds'
    # )
    # queries.append(revenue_query)
    
    # # Top products
    # # TẠO CỘT MỚI DẠNG STRING
    # top_products_df_for_es = metrics['top_products'] \
    #     .withColumn("window_start_str", col("window.start").cast("string"))

    # top_products_query = processor.write_to_elasticsearch(
    #     top_products_df_for_es, # Dùng DataFrame mới
    #     'retail_realtime_products',
    #     f"{checkpoint_base}/products",
    #     doc_id_cols=["window_start_str", "StockCode"], # Dùng cột string mới
    #     trigger_time='30 seconds'
    # )
    # queries.append(top_products_query)
    
    # # Alerts
    # # TẠO CỘT MỚI DẠNG STRING
    # alerts_df_for_es = alerts \
    #     .withColumn("WindowStart_str", col("WindowStart").cast("string")) \
    #     .withColumn("AlertTime_str", col("AlertTime").cast("string"))

    # Revenue metrics
    # Flatten window.* -> WindowStart/WindowEnd + tạo cột string cho doc_id
    revenue_df_for_es = metrics['revenue'] \
        .withColumn("WindowStart", col("window.start")) \
        .withColumn("WindowEnd", col("window.end")) \
        .withColumn("window_start_str", col("window.start").cast("string")) \
        .drop("window")  

    revenue_query = processor.write_to_elasticsearch(
        revenue_df_for_es,  # Dùng DataFrame đã flatten
        'retail_realtime_revenue',
        f"{checkpoint_base}/revenue",
        doc_id_cols=["window_start_str", "Country"],  # Dùng cột string mới
        trigger_time='30 seconds'
    )
    queries.append(revenue_query)

    # Top products
    # Flatten window.* -> WindowStart/WindowEnd + tạo cột string cho doc_id
    top_products_df_for_es = metrics['top_products'] \
        .withColumn("WindowStart", col("window.start")) \
        .withColumn("WindowEnd", col("window.end")) \
        .withColumn("window_start_str", col("window.start").cast("string")) \
        .drop("window")

    top_products_query = processor.write_to_elasticsearch(
        top_products_df_for_es,  # Dùng DataFrame đã flatten
        'retail_realtime_products',
        f"{checkpoint_base}/products",
        doc_id_cols=["window_start_str", "StockCode"],  # Dùng cột string mới
        trigger_time='30 seconds'
    )
    queries.append(top_products_query)

    # Alerts
    # TẠO CỘT MỚI DẠNG STRING
    alerts_df_for_es = alerts \
        .withColumn("WindowStart_str", col("WindowStart").cast("string")) \
        .withColumn("AlertTime_str", col("AlertTime").cast("string"))


    alerts_query = processor.write_to_elasticsearch(
        alerts_df_for_es, # Dùng DataFrame mới
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
        .config("spark.executor.memory", "3g") \
        .getOrCreate()
    
    # Configuration
    KAFKA_SERVERS = "kafka:29092"
    TOPIC = "retail_transactions"
    ES_HOST = "http://elasticsearch:9200"
    CHECKPOINT_BASE = "/opt/spark-data/checkpoints"
    
    # Run streaming job
    run_streaming_job(spark, KAFKA_SERVERS, TOPIC, ES_HOST, CHECKPOINT_BASE)
    
    spark.stop()