"""
Data Preprocessing and Cleaning Module
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataPreprocessor:
    """Tiền xử lý và làm sạch dữ liệu retail"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.stats = {}
    
    def load_data(self, path: str, format: str = 'csv') -> DataFrame:
        """
        Load dữ liệu từ file
        
        Args:
            path: Đường dẫn file
            format: Định dạng file (csv, parquet, json)
        """
        logger.info(f"Loading data from {path} (format: {format})")
        
        if format == 'csv':
            df = self.spark.read.csv(path, header=True, inferSchema=True)
        elif format == 'parquet':
            df = self.spark.read.parquet(path)
        elif format == 'json':
            df = self.spark.read.json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        initial_count = df.count()
        logger.info(f"Loaded {initial_count:,} records")
        
        self.stats['initial_count'] = initial_count
        return df
    
    def clean_transactions(self, df: DataFrame) -> DataFrame:
        """
        Làm sạch dữ liệu giao dịch
        
        - Loại bỏ null values
        - Loại bỏ giao dịch âm (returns) nếu cần
        - Loại bỏ giá trị bất thường
        - Format chuẩn hóa
        """
        logger.info("Starting data cleaning...")
        
        # 1. Remove null CustomerID
        df_clean = df.filter(col('CustomerID').isNotNull())
        null_removed = self.stats['initial_count'] - df_clean.count()
        logger.info(f"Removed {null_removed:,} records with null CustomerID")
        
        # 2. Remove invalid quantities and prices
        df_clean = df_clean.filter(
            (col('Quantity') > 0) & 
            (col('UnitPrice') > 0)
        )
        invalid_removed = self.stats['initial_count'] - null_removed - df_clean.count()
        logger.info(f"Removed {invalid_removed:,} records with invalid Quantity/Price")
        
        # 3. Remove outliers (quantity > 10000 or price > 10000)
        df_clean = df_clean.filter(
            (col('Quantity') <= 10000) & 
            (col('UnitPrice') <= 10000)
        )
        outliers_removed = self.stats['initial_count'] - null_removed - invalid_removed - df_clean.count()
        logger.info(f"Removed {outliers_removed:,} outlier records")
        
        # 4. Add computed columns
        df_clean = df_clean.withColumn(
            'TotalAmount',
            col('Quantity') * col('UnitPrice')
        )
        
        # 5. Convert InvoiceDate to proper timestamp
        df_clean = df_clean.withColumn(
            'InvoiceDate',
            to_timestamp('InvoiceDate')
        )
        
        # 6. Clean StockCode (trim whitespace)
        df_clean = df_clean.withColumn(
            'StockCode',
            trim(col('StockCode'))
        )
        
        # 7. Clean Description
        df_clean = df_clean.withColumn(
            'Description',
            trim(upper(col('Description')))
        )
        
        final_count = df_clean.count()
        self.stats['final_count'] = final_count
        self.stats['removed_count'] = self.stats['initial_count'] - final_count
        self.stats['removal_rate'] = (self.stats['removed_count'] / self.stats['initial_count']) * 100
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Data Cleaning Summary:")
        logger.info(f"Initial records: {self.stats['initial_count']:,}")
        logger.info(f"Final records: {final_count:,}")
        logger.info(f"Removed: {self.stats['removed_count']:,} ({self.stats['removal_rate']:.2f}%)")
        logger.info(f"{'='*60}\n")
        
        return df_clean
    
    def extract_returns(self, df: DataFrame) -> DataFrame:
        """
        Trích xuất các giao dịch trả hàng (Quantity < 0)
        """
        logger.info("Extracting returns data...")
        
        returns_df = df.filter(col('Quantity') < 0)
        returns_count = returns_df.count()
        
        logger.info(f"Found {returns_count:,} return transactions")
        
        return returns_df
    
    def add_temporal_features(self, df: DataFrame) -> DataFrame:
        """
        Thêm các features liên quan đến thời gian
        """
        logger.info("Adding temporal features...")
        
        df_temporal = df \
            .withColumn('Year', year('InvoiceDate')) \
            .withColumn('Month', month('InvoiceDate')) \
            .withColumn('Day', dayofmonth('InvoiceDate')) \
            .withColumn('DayOfWeek', dayofweek('InvoiceDate')) \
            .withColumn('Hour', hour('InvoiceDate')) \
            .withColumn('Quarter', quarter('InvoiceDate')) \
            .withColumn('WeekOfYear', weekofyear('InvoiceDate'))
        
        return df_temporal
    
    def get_data_profile(self, df: DataFrame) -> dict:
        """
        Tạo profile của dữ liệu
        """
        logger.info("Generating data profile...")
        
        profile = {
            'total_records': df.count(),
            'total_customers': df.select('CustomerID').distinct().count(),
            'total_products': df.select('StockCode').distinct().count(),
            'total_countries': df.select('Country').distinct().count(),
            'total_invoices': df.select('InvoiceNo').distinct().count(),
            'date_range': {
                'min': df.agg(min('InvoiceDate')).collect()[0][0],
                'max': df.agg(max('InvoiceDate')).collect()[0][0]
            },
            'total_revenue': df.agg(sum('TotalAmount')).collect()[0][0],
            'avg_transaction_value': df.agg(avg('TotalAmount')).collect()[0][0]
        }
        
        logger.info(f"\n{'='*60}")
        logger.info("Data Profile:")
        for key, value in profile.items():
            logger.info(f"{key}: {value}")
        logger.info(f"{'='*60}\n")
        
        return profile
    
    def save_cleaned_data(self, df: DataFrame, output_path: str, format: str = 'parquet'):
        """
        Lưu dữ liệu đã làm sạch
        """
        logger.info(f"Saving cleaned data to {output_path} (format: {format})")
        
        if format == 'parquet':
            df.write.mode('overwrite').parquet(output_path)
        elif format == 'csv':
            df.write.mode('overwrite').csv(output_path, header=True)
        
        logger.info(f"✅ Data saved successfully")


def run_preprocessing_job(spark, input_path, output_path):
    """
    Main preprocessing job
    """
    logger.info("="*60)
    logger.info("Starting Data Preprocessing Job")
    logger.info("="*60)
    
    preprocessor = DataPreprocessor(spark)
    
    # 1. Load data
    df = preprocessor.load_data(input_path, format='csv')
    
    # 2. Clean data
    df_clean = preprocessor.clean_transactions(df)
    
    # 3. Add temporal features
    df_enriched = preprocessor.add_temporal_features(df_clean)
    
    # 4. Extract returns
    df_returns = preprocessor.extract_returns(df)
    
    # 5. Get profile
    profile = preprocessor.get_data_profile(df_enriched)
    
    # 6. Save
    preprocessor.save_cleaned_data(
        df_enriched, 
        f"{output_path}/cleaned_transactions.parquet"
    )
    
    preprocessor.save_cleaned_data(
        df_returns,
        f"{output_path}/returns_transactions.parquet"
    )
    
    logger.info("="*60)
    logger.info("Preprocessing Job Completed")
    logger.info("="*60)
    
    return df_enriched, df_returns


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("RetailDataPreprocessing") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    INPUT_PATH = "/opt/spark-data/raw/online_retail.csv"
    OUTPUT_PATH = "/opt/spark-data/processed"
    
    run_preprocessing_job(spark, INPUT_PATH, OUTPUT_PATH)
    
    spark.stop()
