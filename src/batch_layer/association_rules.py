"""
Association Rules Mining using FP-Growth
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size
from pyspark.ml.fpm import FPGrowth
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AssociationRulesMiner:
    """Khai phá luật kết hợp từ dữ liệu giao dịch"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def prepare_baskets(self, df, min_basket_size=2):
        """
        Chuẩn bị dữ liệu dưới dạng giỏ hàng (baskets)
        
        Args:
            df: Spark DataFrame với các cột InvoiceNo, StockCode
            min_basket_size: Số lượng items tối thiểu trong một basket
            
        Returns:
            DataFrame với cột 'items' chứa list các StockCode
        """
        logger.info("Preparing transaction baskets...")
        
        # Nhóm các sản phẩm theo hóa đơn
        baskets = df.groupBy('InvoiceNo') \
                    .agg(collect_list('StockCode').alias('items')) \
                    .filter(size(col('items')) >= min_basket_size)
        
        basket_count = baskets.count()
        logger.info(f"Created {basket_count} baskets")
        
        return baskets
    
    def mine_patterns(self, baskets_df, min_support=0.01, min_confidence=0.5):
        """
        Chạy FP-Growth để tìm frequent itemsets và association rules
        
        Args:
            baskets_df: DataFrame chứa baskets
            min_support: Support tối thiểu (0.01 = 1%)
            min_confidence: Confidence tối thiểu (0.5 = 50%)
            
        Returns:
            Tuple (frequent_itemsets_df, association_rules_df)
        """
        logger.info(f"Running FP-Growth with min_support={min_support}, "
                   f"min_confidence={min_confidence}")
        
        # Khởi tạo FP-Growth
        fpGrowth = FPGrowth(
            itemsCol="items",
            minSupport=min_support,
            minConfidence=min_confidence
        )
        
        # Train model
        model = fpGrowth.fit(baskets_df)
        
        # Lấy kết quả
        frequent_itemsets = model.freqItemsets
        association_rules = model.associationRules
        
        # Thống kê
        itemsets_count = frequent_itemsets.count()
        rules_count = association_rules.count()
        
        logger.info(f"Found {itemsets_count} frequent itemsets")
        logger.info(f"Generated {rules_count} association rules")
        
        return frequent_itemsets, association_rules
    
    def format_rules_for_elasticsearch(self, rules_df, product_names_df=None):
        """
        Format association rules để lưu vào Elasticsearch
        
        Args:
            rules_df: DataFrame chứa association rules
            product_names_df: DataFrame với StockCode và Description (optional)
            
        Returns:
            Pandas DataFrame ready for ES
        """
        from pyspark.sql.functions import concat_ws, monotonically_increasing_id
        
        # Add ID và format
        formatted = rules_df \
            .withColumn('rule_id', monotonically_increasing_id()) \
            .withColumn('antecedent_str', concat_ws(', ', col('antecedent'))) \
            .withColumn('consequent_str', concat_ws(', ', col('consequent')))
        
        # Convert to Pandas
        rules_pd = formatted.select(
            'rule_id',
            'antecedent',
            'consequent',
            'antecedent_str',
            'consequent_str',
            'confidence',
            'lift',
            'support'
        ).toPandas()
        
        # Add product names if available
        if product_names_df is not None:
            # TODO: Join với product names để có description
            pass
        
        return rules_pd
    
    def get_top_rules(self, rules_df, top_n=20, sort_by='lift'):
        """
        Lấy top N rules mạnh nhất
        
        Args:
            rules_df: DataFrame chứa rules
            top_n: Số lượng rules trả về
            sort_by: Sắp xếp theo cột nào (lift, confidence, support)
        """
        return rules_df.orderBy(col(sort_by).desc()).limit(top_n)


def run_association_mining_job(spark, input_path, output_path, es_config):
    """
    Job chính để chạy association rules mining
    
    Args:
        spark: SparkSession
        input_path: Đường dẫn đến dữ liệu đầu vào (CSV hoặc Parquet)
        output_path: Đường dẫn lưu kết quả
        es_config: Elasticsearch configuration dict
    """
    logger.info("="*60)
    logger.info("Starting Association Rules Mining Job")
    logger.info("="*60)
    
    # 1. Load data
    logger.info(f"Loading data from {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # 2. Clean data
    df_clean = df.filter(
        (col('Quantity') > 0) & 
        (col('CustomerID').isNotNull()) &
        (col('UnitPrice') > 0)
    ).select('InvoiceNo', 'StockCode', 'Description')
    
    # 3. Create miner
    miner = AssociationRulesMiner(spark)
    
    # 4. Prepare baskets
    baskets = miner.prepare_baskets(df_clean, min_basket_size=2)
    
    # 5. Mine patterns
    frequent_itemsets, association_rules = miner.mine_patterns(
        baskets,
        min_support=0.01,
        min_confidence=0.3
    )
    
    # 6. Get top rules
    top_rules = miner.get_top_rules(association_rules, top_n=100, sort_by='lift')
    
    # 7. Show sample results
    logger.info("\n" + "="*60)
    logger.info("Sample Top Rules:")
    logger.info("="*60)
    top_rules.show(10, truncate=False)
    
    # 8. Save to Parquet
    parquet_path = f"{output_path}/association_rules.parquet"
    logger.info(f"Saving rules to {parquet_path}")
    association_rules.write.mode('overwrite').parquet(parquet_path)
    
    # 9. Save to Elasticsearch
    if es_config:
        logger.info("Saving rules to Elasticsearch...")
        rules_pd = miner.format_rules_for_elasticsearch(association_rules)
        
        from elasticsearch import Elasticsearch, helpers
        es = Elasticsearch([es_config['host']])
        
        # Prepare documents
        actions = [
            {
                "_index": "retail_association_rules",
                "_id": row['rule_id'],
                "_source": row.to_dict()
            }
            for _, row in rules_pd.iterrows()
        ]
        
        # Bulk index
        helpers.bulk(es, actions)
        logger.info(f"Indexed {len(actions)} rules to Elasticsearch")
    
    logger.info("="*60)
    logger.info("Association Rules Mining Job Completed")
    logger.info("="*60)
    
    return association_rules


if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("RetailAssociationRulesMining") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    # Configuration
    INPUT_PATH = "/opt/spark-data/raw/online_retail.csv"
    OUTPUT_PATH = "/opt/spark-data/processed"
    ES_CONFIG = {
        'host': 'http://elasticsearch:9200'
    }
    
    # Run job
    run_association_mining_job(spark, INPUT_PATH, OUTPUT_PATH, ES_CONFIG)
    
    spark.stop()
