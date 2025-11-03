"""
Batch Job Runner - Orchestrates all batch processing jobs
"""
from pyspark.sql import SparkSession
import argparse
import logging
from datetime import datetime

# Import job modules
from data_preprocessing import run_preprocessing_job
from association_rules import run_association_mining_job
from demand_forecasting import run_demand_forecasting_job
from customer_segmentation import run_customer_segmentation_job

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchJobRunner:
    """Orchestrator cho tất cả batch jobs"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.job_results = {}
    
    def run_all_jobs(self):
        """Chạy tất cả jobs theo thứ tự"""
        logger.info("="*80)
        logger.info("STARTING BATCH JOB PIPELINE")
        logger.info(f"Start Time: {datetime.now()}")
        logger.info("="*80)
        
        jobs = [
            ('Preprocessing', self.run_preprocessing),
            ('Association Rules', self.run_association_rules),
            ('Demand Forecasting', self.run_forecasting),
            ('Customer Segmentation', self.run_segmentation)
        ]
        
        for job_name, job_func in jobs:
            try:
                logger.info(f"\n{'='*80}")
                logger.info(f"Starting Job: {job_name}")
                logger.info(f"{'='*80}")
                
                start_time = datetime.now()
                result = job_func()
                end_time = datetime.now()
                
                duration = (end_time - start_time).total_seconds()
                
                self.job_results[job_name] = {
                    'status': 'SUCCESS',
                    'duration': duration,
                    'start_time': start_time,
                    'end_time': end_time,
                    'result': result
                }
                
                logger.info(f"✅ Job '{job_name}' completed successfully in {duration:.2f}s")
                
            except Exception as e:
                logger.error(f"❌ Job '{job_name}' failed: {str(e)}")
                self.job_results[job_name] = {
                    'status': 'FAILED',
                    'error': str(e),
                    'start_time': start_time,
                    'end_time': datetime.now()
                }
                
                if self.config.get('stop_on_error', False):
                    logger.error("Stopping pipeline due to error")
                    break
        
        self.print_summary()
    
    def run_preprocessing(self):
        """Job 1: Data Preprocessing"""
        return run_preprocessing_job(
            self.spark,
            self.config['input_path'],
            self.config['output_path']
        )
    
    def run_association_rules(self):
        """Job 2: Association Rules Mining"""
        return run_association_mining_job(
            self.spark,
            f"{self.config['output_path']}/cleaned_transactions.parquet",
            self.config['output_path'],
            self.config['es_config']
        )
    
    def run_forecasting(self):
        """Job 3: Demand Forecasting"""
        return run_demand_forecasting_job(
            self.spark,
            f"{self.config['output_path']}/cleaned_transactions.parquet",
            self.config['output_path'],
            self.config['es_config']
        )
    
    def run_segmentation(self):
        """Job 4: Customer Segmentation"""
        return run_customer_segmentation_job(
            self.spark,
            self.config['output_path'],
            self.config['output_path'],
            self.config['es_config']
        )
    
    def print_summary(self):
        """In summary của tất cả jobs"""
        logger.info("\n" + "="*80)
        logger.info("BATCH JOB PIPELINE SUMMARY")
        logger.info("="*80)
        
        total_duration = 0
        success_count = 0
        failed_count = 0
        
        for job_name, result in self.job_results.items():
            status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
            duration = result.get('duration', 0)
            total_duration += duration
            
            if result['status'] == 'SUCCESS':
                success_count += 1
                logger.info(f"{status_icon} {job_name}: SUCCESS ({duration:.2f}s)")
            else:
                failed_count += 1
                logger.info(f"{status_icon} {job_name}: FAILED - {result.get('error', 'Unknown')}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Total Jobs: {len(self.job_results)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"Total Duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")
        logger.info(f"End Time: {datetime.now()}")
        logger.info(f"{'='*80}\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Run batch processing jobs')
    parser.add_argument('--input', type=str, required=True, 
                       help='Input data path')
    parser.add_argument('--output', type=str, required=True,
                       help='Output data path')
    parser.add_argument('--es-host', type=str, default='http://elasticsearch:9200',
                       help='Elasticsearch host')
    parser.add_argument('--jobs', type=str, nargs='+',
                       choices=['preprocessing', 'association', 'forecasting', 'segmentation', 'all'],
                       default=['all'],
                       help='Jobs to run')
    parser.add_argument('--stop-on-error', action='store_true',
                       help='Stop pipeline if a job fails')
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("RetailBatchJobRunner") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configuration
    config = {
        'input_path': args.input,
        'output_path': args.output,
        'es_config': {'host': args.es_host},
        'stop_on_error': args.stop_on_error
    }
    
    # Run jobs
    runner = BatchJobRunner(spark, config)
    
    if 'all' in args.jobs:
        runner.run_all_jobs()
    else:
        # Run specific jobs
        if 'preprocessing' in args.jobs:
            runner.run_preprocessing()
        if 'association' in args.jobs:
            runner.run_association_rules()
        if 'forecasting' in args.jobs:
            runner.run_forecasting()
        if 'segmentation' in args.jobs:
            runner.run_segmentation()
        
        runner.print_summary()
    
    spark.stop()


if __name__ == "__main__":
    main()
