"""
Customer Segmentation using RFM Analysis and K-Means Clustering
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomerSegmentationEngine:
    """Phân khúc khách hàng sử dụng RFM và K-Means"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def compute_rfm(self, df: DataFrame, reference_date: str = None) -> DataFrame:
        """
        Tính toán RFM scores
        
        Args:
            df: DataFrame với InvoiceDate, CustomerID, TotalAmount
            reference_date: Ngày tham chiếu (mặc định là max date + 1 day)
            
        Returns:
            DataFrame với Recency, Frequency, Monetary
        """
        logger.info("Computing RFM scores...")
        
        # Tìm ngày tham chiếu
        if reference_date is None:
            max_date = df.agg(max('InvoiceDate')).collect()[0][0]
            reference_date = (max_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"Reference date: {reference_date}")
        
        # Tính RFM
        rfm = df.groupBy('CustomerID').agg(
            # Recency: số ngày từ lần mua cuối đến reference_date
            datediff(lit(reference_date), max('InvoiceDate')).alias('Recency'),
            
            # Frequency: số lượng hóa đơn
            countDistinct('InvoiceNo').alias('Frequency'),
            
            # Monetary: tổng tiền đã chi
            sum('TotalAmount').alias('Monetary')
        )
        
        # Loại bỏ giá trị âm hoặc null
        rfm = rfm.filter(
            (col('Recency') >= 0) & 
            (col('Frequency') > 0) & 
            (col('Monetary') > 0)
        )
        
        customer_count = rfm.count()
        logger.info(f"Computed RFM for {customer_count:,} customers")
        
        return rfm
    
    def create_rfm_scores(self, rfm_df: DataFrame) -> DataFrame:
        """
        Tạo RFM scores (1-5) bằng quantile-based binning
        """
        logger.info("Creating RFM scores (1-5)...")
        
        from pyspark.sql.window import Window
        
        # Sử dụng ntile để chia thành 5 groups
        # Lưu ý: Recency thì nhỏ hơn tốt hơn, nên cần đảo ngược
        rfm_scored = rfm_df \
            .withColumn('R_Score', 6 - ntile(5).over(Window.orderBy(col('Recency')))) \
            .withColumn('F_Score', ntile(5).over(Window.orderBy(col('Frequency')))) \
            .withColumn('M_Score', ntile(5).over(Window.orderBy(col('Monetary'))))
        
        # Tạo RFM_Score tổng hợp
        rfm_scored = rfm_scored.withColumn(
            'RFM_Score',
            concat(col('R_Score'), col('F_Score'), col('M_Score'))
        )
        
        # Tạo segment dựa trên RFM score
        rfm_scored = rfm_scored.withColumn(
            'Segment',
            when((col('R_Score') >= 4) & (col('F_Score') >= 4) & (col('M_Score') >= 4), 'Champions')
            .when((col('R_Score') >= 3) & (col('F_Score') >= 3), 'Loyal Customers')
            .when((col('R_Score') >= 4) & (col('F_Score') <= 2), 'Potential Loyalists')
            .when((col('R_Score') >= 3) & (col('M_Score') >= 4), 'Big Spenders')
            .when((col('R_Score') <= 2) & (col('F_Score') >= 3), 'At Risk')
            .when((col('R_Score') <= 2) & (col('F_Score') <= 2), 'Lost Customers')
            .otherwise('Others')
        )
        
        return rfm_scored
    
    def cluster_customers(self, rfm_df: DataFrame, k: int = 5) -> tuple:
        """
        Clustering khách hàng sử dụng K-Means
        
        Args:
            rfm_df: DataFrame với Recency, Frequency, Monetary
            k: Số clusters
            
        Returns:
            (clustered_df, model)
        """
        logger.info(f"Running K-Means clustering with k={k}...")
        
        # 1. Chuẩn bị features
        assembler = VectorAssembler(
            inputCols=['Recency', 'Frequency', 'Monetary'],
            outputCol='features_raw'
        )
        
        df_assembled = assembler.transform(rfm_df)
        
        # 2. Chuẩn hóa features (StandardScaler)
        scaler = StandardScaler(
            inputCol='features_raw',
            outputCol='features',
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        # 3. K-Means clustering
        kmeans = KMeans(
            featuresCol='features',
            predictionCol='Cluster',
            k=k,
            seed=42,
            maxIter=20
        )
        
        model = kmeans.fit(df_scaled)
        clustered = model.transform(df_scaled)
        
        # 4. Evaluation
        evaluator = ClusteringEvaluator(
            featuresCol='features',
            predictionCol='Cluster',
            metricName='silhouette',
            distanceMeasure='squaredEuclidean'
        )
        
        silhouette = evaluator.evaluate(clustered)
        logger.info(f"Silhouette Score: {silhouette:.4f}")
        
        # 5. Cluster statistics
        cluster_stats = clustered.groupBy('Cluster').agg(
            count('*').alias('CustomerCount'),
            avg('Recency').alias('AvgRecency'),
            avg('Frequency').alias('AvgFrequency'),
            avg('Monetary').alias('AvgMonetary')
        ).orderBy('Cluster')
        
        logger.info("\nCluster Statistics:")
        cluster_stats.show()
        
        return clustered, model
    
    def find_optimal_k(self, rfm_df: DataFrame, k_range: range = range(2, 11)) -> dict:
        """
        Tìm số clusters tối ưu bằng Elbow method
        """
        logger.info(f"Finding optimal k in range {list(k_range)}...")
        
        # Prepare features
        assembler = VectorAssembler(
            inputCols=['Recency', 'Frequency', 'Monetary'],
            outputCol='features_raw'
        )
        df_assembled = assembler.transform(rfm_df)
        
        scaler = StandardScaler(
            inputCol='features_raw',
            outputCol='features',
            withStd=True,
            withMean=True
        )
        df_scaled = scaler.fit(df_assembled).transform(df_assembled)
        
        # Test different k values
        results = {}
        
        for k in k_range:
            kmeans = KMeans(featuresCol='features', k=k, seed=42)
            model = kmeans.fit(df_scaled)
            
            # Within Set Sum of Squared Errors
            wssse = model.summary.trainingCost
            
            # Silhouette score
            predictions = model.transform(df_scaled)
            evaluator = ClusteringEvaluator(featuresCol='features')
            silhouette = evaluator.evaluate(predictions)
            
            results[k] = {
                'wssse': wssse,
                'silhouette': silhouette
            }
            
            logger.info(f"k={k}: WSSSE={wssse:.2f}, Silhouette={silhouette:.4f}")
        
        return results
    
    def label_clusters(self, clustered_df: DataFrame) -> DataFrame:
        """
        Gắn nhãn ý nghĩa cho các clusters
        """
        logger.info("Labeling clusters...")
        
        # Tính cluster characteristics
        cluster_chars = clustered_df.groupBy('Cluster').agg(
            avg('Recency').alias('AvgRecency'),
            avg('Frequency').alias('AvgFrequency'),
            avg('Monetary').alias('AvgMonetary')
        ).collect()
        
        # Tạo mapping dựa trên characteristics
        cluster_labels = {}
        
        for row in cluster_chars:
            cluster_id = row['Cluster']
            recency = row['AvgRecency']
            frequency = row['AvgFrequency']
            monetary = row['AvgMonetary']
            
            # Logic gán nhãn
            if recency < 50 and frequency > 10 and monetary > 1000:
                label = 'VIP Customers'
            elif recency < 100 and frequency > 5:
                label = 'Loyal Customers'
            elif recency < 50 and frequency <= 5:
                label = 'New Customers'
            elif recency > 200 and frequency > 5:
                label = 'At Risk'
            elif recency > 300:
                label = 'Lost Customers'
            else:
                label = 'Regular Customers'
            
            cluster_labels[cluster_id] = label
        
        # Apply labels
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
        label_udf = udf(lambda x: cluster_labels.get(x, 'Unknown'), StringType())
        
        labeled = clustered_df.withColumn(
            'ClusterLabel',
            label_udf(col('Cluster'))
        )
        
        return labeled


def run_customer_segmentation_job(spark, input_path, output_path, es_config):
    """
    Main segmentation job
    """
    logger.info("="*60)
    logger.info("Starting Customer Segmentation Job")
    logger.info("="*60)
    
    # 1. Load data
    df = spark.read.parquet(f"{input_path}/cleaned_transactions.parquet")
    
    # 2. Create segmentation engine
    engine = CustomerSegmentationEngine(spark)
    
    # 3. Compute RFM
    rfm = engine.compute_rfm(df)
    
    # 4. Create RFM scores
    rfm_scored = engine.create_rfm_scores(rfm)
    
    # 5. Find optimal k (optional)
    optimal_k_results = engine.find_optimal_k(rfm_scored, k_range=range(3, 8))
    
    # 6. Cluster customers (sử dụng k=5)
    clustered, model = engine.cluster_customers(rfm_scored, k=5)
    
    # 7. Label clusters
    labeled = engine.label_clusters(clustered)
    
    # 8. Show results
    logger.info("\nSegment Distribution:")
    labeled.groupBy('ClusterLabel').count().show()
    
    # 9. Save results
    output_file = f"{output_path}/customer_segments.parquet"
    logger.info(f"Saving to {output_file}")
    labeled.select(
        'CustomerID', 'Recency', 'Frequency', 'Monetary',
        'R_Score', 'F_Score', 'M_Score', 'RFM_Score',
        'Segment', 'Cluster', 'ClusterLabel'
    ).write.mode('overwrite').parquet(output_file)
    
    # 10. Save to Elasticsearch
    if es_config:
        logger.info("Saving to Elasticsearch...")
        from elasticsearch import Elasticsearch, helpers
        
        es = Elasticsearch([es_config['host']])
        
        segments_pd = labeled.toPandas()
        
        actions = [
            {
                "_index": "retail_customer_segments",
                "_id": row['CustomerID'],
                "_source": {
                    'CustomerID': str(row['CustomerID']),
                    'Recency': int(row['Recency']),
                    'Frequency': int(row['Frequency']),
                    'Monetary': float(row['Monetary']),
                    'RFM_Score': str(row['RFM_Score']),
                    'Segment': row['Segment'],
                    'Cluster': int(row['Cluster']),
                    'ClusterLabel': row['ClusterLabel']
                }
            }
            for _, row in segments_pd.iterrows()
        ]
        
        helpers.bulk(es, actions)
        logger.info(f"Indexed {len(actions)} customer segments")
    
    logger.info("="*60)
    logger.info("Customer Segmentation Job Completed")
    logger.info("="*60)
    
    return labeled


if __name__ == "__main__":
    import pandas as pd
    
    spark = SparkSession.builder \
        .appName("CustomerSegmentation") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    INPUT_PATH = "/opt/spark-data/processed"
    OUTPUT_PATH = "/opt/spark-data/processed"
    ES_CONFIG = {'host': 'http://elasticsearch:9200'}
    
    run_customer_segmentation_job(spark, INPUT_PATH, OUTPUT_PATH, ES_CONFIG)
    
    spark.stop()
