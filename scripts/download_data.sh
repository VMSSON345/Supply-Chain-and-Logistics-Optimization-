#!/bin/bash

echo "=================================================="
echo "  Downloading UCI Online Retail Dataset"
echo "=================================================="

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

DATA_DIR="data/raw"
DATASET_URL="https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
XLSX_FILE="$DATA_DIR/online_retail.xlsx"
CSV_FILE="$DATA_DIR/online_retail.csv"

# Create directory if not exists
mkdir -p $DATA_DIR

echo -e "\n${YELLOW}Step 1: Downloading dataset...${NC}"

# Download dataset
if [ -f "$XLSX_FILE" ]; then
    echo -e "${YELLOW}Dataset already exists. Skipping download.${NC}"
else
    wget -O "$XLSX_FILE" "$DATASET_URL"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Dataset downloaded successfully${NC}"
    else
        echo -e "${RED}✗ Failed to download dataset${NC}"
        exit 1
    fi
fi

echo -e "\n${YELLOW}Step 2: Converting XLSX to CSV...${NC}"

# Convert to CSV using Python
python3 << EOF
import pandas as pd
import sys

try:
    print("Reading Excel file...")
    df = pd.read_excel('$XLSX_FILE')
    
    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    print("Saving to CSV...")
    df.to_csv('$CSV_FILE', index=False)
    
    print("✓ Conversion successful!")
    print(f"CSV file saved to: $CSV_FILE")
    
except Exception as e:
    print(f"✗ Error: {e}")
    sys.exit(1)
EOF

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Dataset ready: $CSV_FILE${NC}"
    
    # Show dataset info
    echo -e "\n${YELLOW}Dataset Information:${NC}"
    python3 << EOF
import pandas as pd
df = pd.read_csv('$CSV_FILE')
print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")
print(f"Unique customers: {df['CustomerID'].nunique():,}")
print(f"Unique products: {df['StockCode'].nunique():,}")
print(f"Total revenue: £{(df['Quantity'] * df['UnitPrice']).sum():,.2f}")
EOF
    
else
    echo -e "${RED}✗ Conversion failed${NC}"
    exit 1
fi

echo -e "\n${GREEN}=================================================="
echo -e "  Dataset Download Complete!"
echo -e "==================================================${NC}"

