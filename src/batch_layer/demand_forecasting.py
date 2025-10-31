"""
Demand Forecasting using Prophet (Time Series)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
import pandas as pd
from prophet import Prophet
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DemandForecaster:
    """Dự báo nhu cầu sản phẩm sử dụng Prophet"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def prepare_timeseries_data(self, df):
        """
        Chuẩn bị dữ liệu time series: aggregate theo ngày và sản phẩm
        
        Args:
            df: Spark DataFrame với InvoiceDate, StockCode, Quantity
            
        Returns:
            DataFrame với daily sales per product
        """
        logger.info("Preparing time series data...")
        
        from pyspark.sql.functions import to_date
        
        # Aggregate daily sales
        daily_sales = df.groupBy(
            'StockCode',
            to_date('InvoiceDate').alias('Date')
        ).agg(
            _sum('Quantity').alias('Quantity'),
            _sum(col('Quantity') * col('UnitPrice')).alias('Revenue')
        )
        
        return daily_sales
    
    def forecast_product_demand(self, product_df, periods=30):
        """
        Dự báo nhu cầu cho một sản phẩm sử dụng Prophet
        
        Args:
            product_df: Pandas DataFrame với columns ['Date', 'Quantity']
            periods: Số ngày muốn dự báo
            
        Returns:
            Pandas DataFrame với forecast
        """
        # Chuẩn bị dữ liệu cho Prophet (cần columns 'ds' và 'y')
        prophet_df = product_df.rename(columns={'Date': 'ds', 'Quantity': 'y'})
        prophet_df = prophet_df[['ds', 'y']].sort_values('ds')
        
        # Khởi tạo và fit model
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=True,
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.05
        )
        
        model.fit(prophet_df)
        
        # Tạo future dataframe
        future = model.make_future_dataframe(periods=periods)
        
        # Predict
        forecast = model.predict(future)
        
        # Lấy kết quả quan trọng
        result = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periods)
        result['yhat'] = result['yhat'].clip(lower=0)  # Không cho phép âm
        
        return result
    
    def forecast_all_products(self, daily_sales_df, top_n=100, periods=30):
        """
        Dự báo cho top N sản phẩm
        
        Args:
            daily_sales_df: Spark DataFrame
            top_n: Số lượng sản phẩm dự báo
            periods: Số ngày dự báo
            
        Returns:
            Pandas DataFrame với tất cả forecasts
        """
        logger.info(f"Forecasting demand for top {top_n} products...")
        
        # Tìm top products theo tổng doanh số
        top_products = daily_sales_df.groupBy('StockCode') \
            .agg(_sum('Quantity').alias('TotalQuantity')) \
            .orderBy(col('TotalQuantity').desc()) \
            .limit(top_n) \
            .select('StockCode') \
            .rdd.flatMap(lambda x: x).collect()
        
        logger.info(f"Selected {len(top_products)} products for forecasting")
        
        # Convert to Pandas
        df_pd = daily_sales_df.toPandas()
        
        all_forecasts = []
        
        for i, stock_code in enumerate(top_products):
            try:
                # Filter data cho sản phẩm này
                product_data = df_pd[df_pd['StockCode'] == stock_code][['Date', 'Quantity']]
                
                if len(product_data) < 30:  # Cần ít nhất 30 ngày dữ liệu
                    logger.warning(f"Skipping {stock_code}: insufficient data")
                    continue
                
                # Forecast
                forecast = self.forecast_product_demand(product_data, periods)
                forecast['StockCode'] = stock_code
                
                all_forecasts.append(forecast)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i+1}/{len(top_products)} products forecasted")
                    
            except Exception as e:
                logger.error(f"Error forecasting {stock_code}: {e}")
                continue
        
        # Combine all forecasts
        result_df = pd.concat(all_forecasts, ignore_index=True)
        
        logger.info(f"Completed forecasting for {len(all_forecasts)} products")
        
        return result_df
    
    def calculate_safety_stock(self, forecast_df, service_level=0.95):
        """
        Tính toán safety stock dựa trên forecast
        
        Args:
            forecast_df: DataFrame với forecast
            service_level: Mức dịch vụ mong muốn (0.95 = 95%)
            
        Returns:
            DataFrame với safety stock recommendations
        """
        from scipy import stats
        
        z_score = stats.norm.ppf(service_level)
        
        # Group by product và tính safety stock
        safety_stock = forecast_df.groupby('StockCode').agg({
            'yhat': 'mean',
            'yhat_upper': 'max',
            'yhat_lower': 'min'
        }).reset_index()
        
        # Safety stock = Z * Standard Deviation of demand
        safety_stock['std_demand'] = (safety_stock['yhat_upper'] - safety_stock['yhat_lower']) / 4
        safety_stock['safety_stock'] = z_score * safety_stock['std_demand']
        safety_stock['reorder_point'] = safety_stock['yhat'] + safety_stock['safety_stock']
        
        return safety_stock


def run_demand_forecasting_job(spark, input_path, output_path, es_config):
    """
    Job chính để chạy demand forecasting
    """
    logger.info("="*60)
    logger.info("Starting Demand Forecasting Job")
    logger.info("="*60)
    
    # 1. Load data
    logger.info(f"Loading data from {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # 2. Clean data
    from pyspark.sql.functions import to_timestamp
    df_clean = df.filter(
        (col('Quantity') > 0) & 
        (col('UnitPrice') > 0)
    ).withColumn('InvoiceDate', to_timestamp('InvoiceDate'))
    
    # 3. Create forecaster
    forecaster = DemandForecaster(spark)
    
    # 4. Prepare time series data
    daily_sales = forecaster.prepare_timeseries_data(df_clean)
    daily_sales.cache()
    
    logger.info(f"Total daily records: {daily_sales.count()}")
    
    # 5. Forecast for top products
    forecasts = forecaster.forecast_all_products(
        daily_sales,
        top_n=50,  # Top 50 sản phẩm
        periods=30  # Dự báo 30 ngày
    )
    
    # 6. Calculate safety stock
    safety_stock = forecaster.calculate_safety_stock(forecasts, service_level=0.95)
    
    # 7. Save results
    forecasts_path = f"{output_path}/demand_forecasts.csv"
    safety_stock_path = f"{output_path}/safety_stock.csv"
    
    logger.info(f"Saving forecasts to {forecasts_path}")
    forecasts.to_csv(forecasts_path, index=False)
    
    logger.info(f"Saving safety stock to {safety_stock_path}")
    safety_stock.to_csv(safety_stock_path, index=False)
    
    # 8. Save to Elasticsearch
    if es_config:
        logger.info("Saving forecasts to Elasticsearch...")
        from elasticsearch import Elasticsearch, helpers
        es = Elasticsearch([es_config['host']])
        
        # Prepare forecast documents
        forecast_actions = [
            {
                "_index": "retail_demand_forecasts",
                "_id": f"{row['StockCode']}_{row['ds'].strftime('%Y-%m-%d')}",
                "_source": {
                    'StockCode': row['StockCode'],
                    'Date': row['ds'].isoformat(),
                    'ForecastQuantity': float(row['yhat']),
                    'LowerBound': float(row['yhat_lower']),
                    'UpperBound': float(row['yhat_upper'])
                }
            }
            for _, row in forecasts.iterrows()
        ]
        
        helpers.bulk(es, forecast_actions)
        logger.info(f"Indexed {len(forecast_actions)} forecast records")
        
        # Prepare safety stock documents
        safety_actions = [
            {
                "_index": "retail_safety_stock",
                "_id": row['StockCode'],
                "_source": row.to_dict()
            }
            for _, row in safety_stock.iterrows()
        ]
        
        helpers.bulk(es, safety_actions)
        logger.info(f"Indexed {len(safety_actions)} safety stock records")
    
    logger.info("="*60)
    logger.info("Demand Forecasting Job Completed")
    logger.info("="*60)
    
    return forecasts, safety_stock


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("RetailDemandForecasting") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    INPUT_PATH = "/opt/spark-data/raw/online_retail.csv"
    OUTPUT_PATH = "/opt/spark-data/processed"
    ES_CONFIG = {'host': 'http://elasticsearch:9200'}
    
    run_demand_forecasting_job(spark, INPUT_PATH, OUTPUT_PATH, ES_CONFIG)
    
    spark.stop()
