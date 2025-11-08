#!/bin/bash

echo "=================================================="
echo "  Running Batch Processing Jobs"
echo "=================================================="

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SPARK_MASTER="spark://spark-master:7077"
INPUT_PATH="/opt/spark-data/raw/online_retail.csv"
OUTPUT_PATH="/opt/spark-data/processed"

# Function to run Spark job
run_spark_job() {
    local job_name=$1
    local script_path=$2
    
    echo -e "\n${YELLOW}Running: $job_name${NC}"
    
    docker exec spark-master spark-submit \
        --master $SPARK_MASTER \
        --executor-memory 4G \
        --driver-memory 4G \
        --conf spark.sql.adaptive.enabled=true \
        $script_path
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $job_name completed successfully${NC}"
    else
        echo -e "${RED}✗ $job_name failed${NC}"
        return 1
    fi
}

# Job 1: Data Preprocessing
sleep 5
echo -e "\n${YELLOW}===== Job 1: Data Preprocessing =====${NC}"
run_spark_job "Data Preprocessing" "/opt/spark-apps/batch_layer/data_preprocessing.py"

sleep 5
# Job 2: Association Rules Mining
echo -e "\n${YELLOW}===== Job 2: Association Rules Mining =====${NC}"
run_spark_job "Association Rules" "/opt/spark-apps/batch_layer/association_rules.py"

sleep 5
# Job 3: Demand Forecasting
echo -e "\n${YELLOW}===== Job 3: Demand Forecasting =====${NC}"
run_spark_job "Demand Forecasting" "/opt/spark-apps/batch_layer/demand_forecasting.py"

sleep 5
# Job 4: Customer Segmentation
echo -e "\n${YELLOW}===== Job 4: Customer Segmentation =====${NC}"
run_spark_job "Customer Segmentation" "/opt/spark-apps/batch_layer/customer_segmentation.py"

echo -e "\n${GREEN}=================================================="
echo -e "  All Batch Jobs Completed!"
echo -e "==================================================${NC}"
