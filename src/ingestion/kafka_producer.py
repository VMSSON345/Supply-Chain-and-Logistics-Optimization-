"""
Kafka Producer: Phát lại dữ liệu CSV vào Kafka topic
"""
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetailDataSimulator:
    """Simulator phát lại giao dịch retail vào Kafka"""
    
    def __init__(self, 
                 csv_path: str,
                 kafka_bootstrap_servers: str = 'localhost:9092',
                 topic_name: str = 'retail_transactions',
                 speed_multiplier: float = 1.0):
        """
        Args:
            csv_path: Đường dẫn file CSV
            kafka_bootstrap_servers: Kafka broker address
            topic_name: Tên topic để gửi dữ liệu
            speed_multiplier: Tốc độ phát lại (1.0 = real-time, 10.0 = nhanh gấp 10 lần)
        """
        self.csv_path = csv_path
        self.topic_name = topic_name
        self.speed_multiplier = speed_multiplier
        
        # Khởi tạo Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=5
        )
        
        logger.info(f"Kafka Producer initialized for topic: {topic_name}")
    
    def load_and_clean_data(self) -> pd.DataFrame:
        """Load và làm sạch dữ liệu"""
        logger.info(f"Loading data from {self.csv_path}")
        
        df = pd.read_csv(self.csv_path, encoding='ISO-8859-1')
        
        # Làm sạch dữ liệu
        df = df.dropna(subset=['CustomerID'])
        df = df[df['Quantity'] > 0]  # Loại bỏ returns
        df = df[df['UnitPrice'] > 0]
        
        # Convert InvoiceDate to datetime
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        df = df.sort_values('InvoiceDate')
        
        logger.info(f"Loaded {len(df)} transactions")
        return df
    
    def send_transaction(self, transaction: dict):
        """Gửi một giao dịch vào Kafka"""
        try:
            # Sử dụng InvoiceNo làm key để partition
            future = self.producer.send(
                self.topic_name,
                key=transaction['InvoiceNo'],
                value=transaction
            )
            
            # Block để đảm bảo message được gửi thành công
            record_metadata = future.get(timeout=10)
            
            return True
        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
            return False
    
    def simulate_streaming(self, batch_size: int = 100, delay_ms: int = 100):
        """
        Phát lại dữ liệu theo thời gian thực
        
        Args:
            batch_size: Số lượng transactions gửi mỗi batch
            delay_ms: Độ trễ giữa các batch (milliseconds)
        """
        df = self.load_and_clean_data()
        
        total_transactions = len(df)
        sent_count = 0
        failed_count = 0
        
        logger.info(f"Starting data simulation with {total_transactions} transactions")
        logger.info(f"Speed multiplier: {self.speed_multiplier}x")
        
        start_time = time.time()
        
        # Gửi dữ liệu theo batch
        for i in range(0, total_transactions, batch_size):
            batch = df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                transaction = {
                    'InvoiceNo': str(row['InvoiceNo']),
                    'StockCode': str(row['StockCode']),
                    'Description': str(row['Description']),
                    'Quantity': int(row['Quantity']),
                    'InvoiceDate': row['InvoiceDate'].isoformat(),
                    'UnitPrice': float(row['UnitPrice']),
                    'CustomerID': str(int(row['CustomerID'])),
                    'Country': str(row['Country']),
                    'TotalAmount': float(row['Quantity'] * row['UnitPrice']),
                    'EventTime': datetime.now().isoformat()
                }
                
                if self.send_transaction(transaction):
                    sent_count += 1
                else:
                    failed_count += 1
            
            # Progress logging
            if (i + batch_size) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                logger.info(f"Progress: {sent_count}/{total_transactions} "
                          f"({sent_count/total_transactions*100:.1f}%) "
                          f"Rate: {rate:.0f} msg/s")
            
            # Delay giữa các batch
            time.sleep(delay_ms / 1000.0 / self.speed_multiplier)
        
        # Đảm bảo tất cả messages đã được gửi
        self.producer.flush()
        
        elapsed = time.time() - start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"Simulation completed!")
        logger.info(f"Total sent: {sent_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"Time elapsed: {elapsed:.2f}s")
        logger.info(f"Average rate: {sent_count/elapsed:.0f} msg/s")
        logger.info(f"{'='*60}")
    
    def close(self):
        """Đóng producer"""
        self.producer.close()
        logger.info("Kafka Producer closed")


def main():
    """Main function để chạy simulator"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Retail Data Kafka Simulator')
    parser.add_argument('--csv', type=str, required=True, 
                       help='Path to CSV file')
    parser.add_argument('--kafka', type=str, default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='retail_transactions',
                       help='Kafka topic name')
    parser.add_argument('--speed', type=float, default=10.0,
                       help='Speed multiplier (default: 10x)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for sending')
    parser.add_argument('--delay', type=int, default=100,
                       help='Delay between batches (ms)')
    
    args = parser.parse_args()
    
    simulator = RetailDataSimulator(
        csv_path=args.csv,
        kafka_bootstrap_servers=args.kafka,
        topic_name=args.topic,
        speed_multiplier=args.speed
    )
    
    try:
        simulator.simulate_streaming(
            batch_size=args.batch_size,
            delay_ms=args.delay
        )
    except KeyboardInterrupt:
        logger.info("\nSimulation interrupted by user")
    finally:
        simulator.close()


if __name__ == "__main__":
    main()
