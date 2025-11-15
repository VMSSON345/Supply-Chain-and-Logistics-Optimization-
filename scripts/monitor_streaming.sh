#!/bin/bash

echo "=================================================="
echo "  Streaming Job Monitor"
echo "=================================================="

while true; do
    echo "=== Kafka Topic Status ==="
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic retail_transactions \
        --time -1 2>/dev/null || echo "Topic not ready"
    
    echo ""
    echo "=== Elasticsearch Document Counts ==="
    curl -s http://localhost:9200/_cat/indices/retail_* | awk '{print $3, $7}'
    
    echo ""
    echo "=== Spark Streaming Status ==="
    echo "Check http://localhost:4040 for Spark UI"
    
    echo ""
    echo "Press Ctrl+C to stop monitoring"
    sleep 5
done
