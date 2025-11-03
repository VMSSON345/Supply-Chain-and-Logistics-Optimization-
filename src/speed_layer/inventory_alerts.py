"""
Inventory Alert System - Real-time anomaly detection
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InventoryAlertSystem:
    """Hệ thống cảnh báo tồn kho real-time"""
    
    def __init__(self, alert_config: dict = None):
        self.config = alert_config or {
            'high_demand_threshold': 2.5,
            'low_stock_threshold': 0.3,
            'price_spike_threshold': 1.5,
            'return_rate_threshold': 0.2
        }
    
    def detect_high_demand_alert(self, stream_df, window_duration="15 minutes"):
        """
        Phát hiện sản phẩm có nhu cầu tăng đột biến
        
        Logic: So sánh current demand với historical average
        """
        logger.info("Setting up high demand alerts...")
        
        # Tính current demand
        current_demand = stream_df \
            .withWatermark("EventTime", "20 minutes") \
            .groupBy(
                window("EventTime", window_duration, "5 minutes"),
                "StockCode",
                "Description"
            ).agg(
                sum("Quantity").alias("CurrentQuantity"),
                count("*").alias("CurrentTransactions"),
                avg("Quantity").alias("AvgQuantity"),
                stddev("Quantity").alias("StdQuantity")
            )
        
        # Detect anomaly: CurrentQuantity > threshold * AvgQuantity
        alerts = current_demand \
            .filter(
                col("CurrentQuantity") > 
                (col("AvgQuantity") * self.config['high_demand_threshold'])
            ) \
            .select(
                col("window.start").alias("WindowStart"),
                col("window.end").alias("WindowEnd"),
                "StockCode",
                "Description",
                "CurrentQuantity",
                "AvgQuantity",
                "CurrentTransactions",
                lit("HIGH_DEMAND").alias("AlertType"),
                lit("Product experiencing unusually high demand").alias("Message"),
                current_timestamp().alias("AlertTime"),
                ((col("CurrentQuantity") / col("AvgQuantity")) - 1).alias("DemandIncreasePct")
            )
        
        return alerts
    
    def detect_stock_out_risk(self, stream_df, inventory_df=None):
        """
        Cảnh báo nguy cơ hết hàng
        
        Note: Cần join với inventory data (nếu có)
        """
        logger.info("Setting up stock-out risk alerts...")
        
        # Tính sell rate
        sell_rate = stream_df \
            .withWatermark("EventTime", "30 minutes") \
            .groupBy(
                window("EventTime", "30 minutes"),
                "StockCode",
                "Description"
            ).agg(
                sum("Quantity").alias("SoldQuantity"),
                (sum("Quantity") / 0.5).alias("HourlySellRate")  # 30min -> hourly
            )
        
        # Nếu có inventory data, join để tính time to stock-out
        if inventory_df is not None:
            stock_out_risk = sell_rate.join(
                inventory_df,
                on="StockCode",
                how="inner"
            ).select(
                col("window.start").alias("WindowStart"),
                "StockCode",
                "Description",
                "SoldQuantity",
                "HourlySellRate",
                "CurrentStock",
                (col("CurrentStock") / col("HourlySellRate")).alias("HoursToStockOut"),
                lit("STOCK_OUT_RISK").alias("AlertType"),
                current_timestamp().alias("AlertTime")
            ).filter(col("HoursToStockOut") < 24)  # Alert if < 24 hours
            
            return stock_out_risk
        else:
            # Chỉ cảnh báo dựa trên high sell rate
            high_sell_rate = sell_rate \
                .filter(col("HourlySellRate") > 100) \
                .select(
                    col("window.start").alias("WindowStart"),
                    "StockCode",
                    "Description",
                    "HourlySellRate",
                    lit("HIGH_SELL_RATE").alias("AlertType"),
                    lit("Product selling at high rate - check inventory").alias("Message"),
                    current_timestamp().alias("AlertTime")
                )
            
            return high_sell_rate
    
    def detect_price_anomalies(self, stream_df, window_duration="20 minutes"):
        """
        Phát hiện giá bất thường
        """
        logger.info("Setting up price anomaly detection...")
        
        price_stats = stream_df \
            .withWatermark("EventTime", "30 minutes") \
            .groupBy(
                window("EventTime", window_duration),
                "StockCode",
                "Description"
            ).agg(
                avg("UnitPrice").alias("CurrentAvgPrice"),
                min("UnitPrice").alias("MinPrice"),
                max("UnitPrice").alias("MaxPrice"),
                stddev("UnitPrice").alias("StdPrice")
            )
        
        # Detect anomalies: MaxPrice > threshold * CurrentAvgPrice
        price_alerts = price_stats \
            .filter(
                (col("MaxPrice") > col("CurrentAvgPrice") * self.config['price_spike_threshold']) |
                (col("MinPrice") < col("CurrentAvgPrice") * (1 - self.config['price_spike_threshold'] + 1))
            ) \
            .select(
                col("window.start").alias("WindowStart"),
                "StockCode",
                "Description",
                "CurrentAvgPrice",
                "MinPrice",
                "MaxPrice",
                lit("PRICE_ANOMALY").alias("AlertType"),
                lit("Unusual price variation detected").alias("Message"),
                current_timestamp().alias("AlertTime")
            )
        
        return price_alerts
    
    def detect_fraudulent_patterns(self, stream_df):
        """
        Phát hiện patterns đáng ngờ (fraud detection)
        
        - Giao dịch giá trị cao bất thường
        - Số lượng bất thường
        - Patterns mua hàng đáng ngờ
        """
        logger.info("Setting up fraud detection...")
        
        # Tính customer spending patterns
        customer_patterns = stream_df \
            .withWatermark("EventTime", "1 hour") \
            .groupBy(
                window("EventTime", "1 hour"),
                "CustomerID",
                "Country"
            ).agg(
                sum("TotalAmount").alias("HourlySpend"),
                count("*").alias("HourlyTransactions"),
                max("TotalAmount").alias("MaxTransaction"),
                avg("TotalAmount").alias("AvgTransaction")
            )
        
        # Flag suspicious patterns
        fraud_alerts = customer_patterns \
            .filter(
                (col("MaxTransaction") > col("AvgTransaction") * 5) |  # Single large transaction
                (col("HourlyTransactions") > 50) |  # Too many transactions
                (col("HourlySpend") > 10000)  # Unusually high spend
            ) \
            .select(
                col("window.start").alias("WindowStart"),
                "CustomerID",
                "Country",
                "HourlySpend",
                "HourlyTransactions",
                "MaxTransaction",
                lit("SUSPICIOUS_ACTIVITY").alias("AlertType"),
                lit("Unusual customer activity detected").alias("Message"),
                current_timestamp().alias("AlertTime")
            )
        
        return fraud_alerts
    
    def create_alert_summary(self, alerts_df):
        """
        Tạo summary của alerts
        """
        summary = alerts_df \
            .groupBy("AlertType") \
            .agg(
                count("*").alias("AlertCount"),
                collect_list("StockCode").alias("AffectedProducts")
            )
        
        return summary


def setup_all_alerts(stream_df, alert_config=None):
    """
    Setup tất cả alert systems
    """
    alert_system = InventoryAlertSystem(alert_config)
    
    alerts = {
        'high_demand': alert_system.detect_high_demand_alert(stream_df),
        'stock_out_risk': alert_system.detect_stock_out_risk(stream_df),
        'price_anomalies': alert_system.detect_price_anomalies(stream_df),
        'fraud_detection': alert_system.detect_fraudulent_patterns(stream_df)
    }
    
    return alerts
