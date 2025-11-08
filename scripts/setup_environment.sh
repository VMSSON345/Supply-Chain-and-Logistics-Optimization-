#!/bin/bash

echo "=================================================="
echo "  Retail Analytics Big Data - Environment Setup"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if running as root
if [ "$EUID" -eq 0 ]; then 
    echo -e "${RED}Please do not run as root${NC}"
    exit 1
fi

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

echo -e "\n${YELLOW}Step 1: Checking prerequisites...${NC}"

# Check Docker
if command_exists docker; then
    echo -e "${GREEN}✓ Docker is installed${NC}"
    docker --version
else
    echo -e "${RED}✗ Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check Docker Compose
if command_exists docker-compose; then
    echo -e "${GREEN}✓ Docker Compose is installed${NC}"
    docker-compose --version
else
    echo -e "${RED}✗ Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

# Check Python
if command_exists python3; then
    echo -e "${GREEN}✓ Python 3 is installed${NC}"
    python3 --version
else
    echo -e "${RED}✗ Python 3 is not installed. Please install Python 3.8+ first.${NC}"
    exit 1
fi

# Check pip
if command_exists pip3; then
    echo -e "${GREEN}✓ pip3 is installed${NC}"
else
    echo -e "${RED}✗ pip3 is not installed. Installing...${NC}"
    sudo apt-get update
    sudo apt-get install -y python3-pip
fi

echo -e "\n${YELLOW}Step 2: Creating project directories...${NC}"

# Create directories
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/checkpoints
mkdir -p logs
mkdir -p config

echo -e "${GREEN}✓ Directories created${NC}"

echo -e "\n${YELLOW}Step 3: Setting up Python virtual environment...${NC}"

# Create virtual environment
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
else
    echo -e "${YELLOW}Virtual environment already exists${NC}"
fi

# Activate virtual environment
source venv/bin/activate

echo -e "\n${YELLOW}Step 4: Installing Python dependencies...${NC}"

# Upgrade pip
pip install --upgrade pip

# Install requirements
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo -e "${GREEN}✓ Python dependencies installed${NC}"
else
    echo -e "${RED}✗ requirements.txt not found${NC}"
    exit 1
fi

echo -e "\n${YELLOW}Step 5: Setting up Docker containers...${NC}"

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}✗ docker-compose.yml not found${NC}"
    exit 1
fi

# Start Docker containers
docker-compose up -d

echo -e "${GREEN}✓ Docker containers starting...${NC}"
echo -e "${YELLOW}Waiting for services to be ready (60 seconds)...${NC}"

# Wait for services
sleep 60

# Check service health
echo -e "\n${YELLOW}Step 6: Checking service health...${NC}"

# Check Kafka
if docker-compose ps | grep -q "kafka.*Up"; then
    echo -e "${GREEN}✓ Kafka is running${NC}"
else
    echo -e "${RED}✗ Kafka failed to start${NC}"
fi

# Check Elasticsearch
if curl -s http://localhost:9200 >/dev/null; then
    echo -e "${GREEN}✓ Elasticsearch is running${NC}"
else
    echo -e "${RED}✗ Elasticsearch failed to start${NC}"
fi

# Check Spark
if docker-compose ps | grep -q "spark-master.*Up"; then
    echo -e "${GREEN}✓ Spark Master is running${NC}"
else
    echo -e "${RED}✗ Spark Master failed to start${NC}"
fi

echo -e "\n${YELLOW}Step 7: Initializing Elasticsearch indices...${NC}"

# Run Elasticsearch initialization
python scripts/init_elasticsearch.py

echo -e "\n${GREEN}=================================================="
echo -e "  Setup Complete!"
echo -e "==================================================${NC}"

echo -e "\n${YELLOW}Service URLs:${NC}"
echo "  - Elasticsearch: http://localhost:9200"
echo "  - Kibana: http://localhost:5601"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Kafka: localhost:9092"

echo -e "\n${YELLOW}Next Steps:${NC}"
echo "  1. Download dataset: ./scripts/download_data.sh"
echo "  2. Run batch processing: docker exec -it spark-master spark-submit ..."
echo "  3. Start streaming: python src/ingestion/kafka_producer.py ..."
echo "  4. Launch dashboard: streamlit run dashboard/app.py"

echo -e "\n${YELLOW}To view logs:${NC}"
echo "  docker-compose logs -f [service-name]"

echo -e "\n${YELLOW}To stop services:${NC}"
echo "  docker-compose down"

echo -e "\n${GREEN}Happy analyzing! 🚀${NC}"
