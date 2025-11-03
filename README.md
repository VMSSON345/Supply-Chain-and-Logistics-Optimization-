# 🛒 Retail Analytics Big Data System

A complete Lambda Architecture implementation for retail data analytics using Apache Spark, Kafka, Elasticsearch, and Streamlit.

## 📋 Overview

This project implements a comprehensive retail analytics system that processes transactional data using both batch and real-time processing to provide:

- 📊 Real-time revenue and sales metrics
- 📈 30-day demand forecasting
- 🛒 Market basket analysis (association rules)
- 📦 Inventory optimization
- 👥 Customer segmentation

## 🏗️ Architecture

┌─────────────┐
│ Data Source │ UCI Online Retail Dataset
└──────┬──────┘
│
├──────────────┬─────────────────┐
│ │ │
┌───▼────┐ ┌────▼─────┐ ┌─────▼────┐
│ Kafka │ │ HDFS/S3 │ │ Batch │
└───┬────┘ └────┬─────┘ │Processing│
│ │ └─────┬────┘
│ │ │
┌───▼─────┐ ┌───▼──────┐ ┌─────▼─────┐
│ Spark │ │ Spark │ │ ES │
│Streaming│ │ Batch │ │ Serving │
└───┬─────┘ └────┬─────┘ └─────┬─────┘
│ │ │
└─────────────┴────────────────┤
│
┌─────────▼──────────┐
│ Streamlit Dashboard│
└────────────────────┘

text

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- 8GB+ RAM
- 20GB+ disk space

### Installation

1. Clone repository
   git clone https://github.com/your-repo/retail-analytics-bigdata.git
   cd retail-analytics-bigdata

2. Run setup script
   chmod +x scripts/setup_environment.sh
   ./scripts/setup_environment.sh

3. Download dataset
   chmod +x scripts/download_data.sh
   ./scripts/download_data.sh

4. Initialize Elasticsearch
   python scripts/init_elasticsearch.py

text

### Running the System

1. Start infrastructure
   docker-compose up -d

2. Run batch processing jobs
   chmod +x scripts/run_batch_jobs.sh
   ./scripts/run_batch_jobs.sh

3. Start data simulator
   python src/ingestion/kafka_producer.py
   --csv data/raw/online_retail.csv
   --speed 10.0

4. Start streaming processor
   docker exec -it spark-master spark-submit
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
   /opt/spark-apps/speed_layer/streaming_processor.py

5. Launch dashboard
   streamlit run dashboard/app.py

text

## 📊 Dashboard

Access the dashboard at: `http://localhost:8501`

### Features

1. **Real-time Overview**

   - Live revenue metrics
   - Top selling products
   - Country-wise distribution

2. **Demand Forecasting**

   - 30-day predictions per product
   - Confidence intervals
   - Export capabilities

3. **Market Basket Analysis**

   - Association rules
   - Product recommendations
   - Network visualization

4. **Inventory Optimization**
   - Safety stock calculations
   - Real-time alerts
   - Reorder point recommendations

## 🛠️ Technology Stack

- **Data Ingestion:** Apache Kafka
- **Stream Processing:** Spark Structured Streaming
- **Batch Processing:** Apache Spark (PySpark)
- **Machine Learning:** Spark MLlib, Prophet
- **Storage:** HDFS, Elasticsearch
- **Visualization:** Streamlit, Plotly
- **Orchestration:** Docker Compose

## 📁 Project Structure

retail-analytics-bigdata/
├── config/ # Configuration files
├── data/ # Data directory
├── dashboard/ # Streamlit dashboard
├── scripts/ # Setup scripts
├── src/
│ ├── batch_layer/ # Batch processing jobs
│ ├── speed_layer/ # Streaming jobs
│ ├── serving_layer/ # API & ES client
│ ├── ingestion/ # Data producers
│ └── utils/ # Utilities
├── tests/ # Unit tests
├── docker-compose.yml
├── requirements.txt
└── README.md

text

## 🧪 Testing

Run all tests
python -m pytest tests/

Run specific test
python tests/test_batch_processing.py

text

## 📈 Performance

- **Batch Processing:** ~1M records in 30 minutes
- **Stream Processing:** ~10K records/second
- **Dashboard Latency:** <2 seconds
- **ES Query Time:** <100ms

## 🔧 Configuration

Edit `config/config.yaml` to customize:

- Spark resources
- Kafka topics
- ES indices
- Algorithm parameters
- Alert thresholds

## 🐛 Troubleshooting

### Services not starting

Check Docker logs
docker-compose logs -f [service-name]

Restart services
docker-compose restart

text

### Elasticsearch connection issues

Check ES health
curl http://localhost:9200/\_cluster/health

text

### Spark job failures

Check Spark UI
open http://localhost:8080

View executor logs
docker exec spark-master ls /opt/spark/work/

text

## 📚 Documentation

- [Architecture Details](docs/ARCHITECTURE.md)
- [API Reference](docs/API.md)
- [Deployment Guide](docs/DEPLOYMENT.md)
- [Tuning Guide](docs/TUNING.md)

## 🤝 Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md)

## 📄 License

MIT License - see [LICENSE](LICENSE)

## 👥 Authors

- Your Name - [your-email@example.com]

## 🙏 Acknowledgments

- UCI Machine Learning Repository for the dataset
- Apache Spark community
- Streamlit team

## 📞 Support

- Issues: [GitHub Issues](https://github.com/your-repo/issues)
- Email: support@example.com
