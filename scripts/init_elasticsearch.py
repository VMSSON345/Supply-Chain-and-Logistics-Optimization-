#!/usr/bin/env python3
"""
Initialize Elasticsearch Indices
"""
from elasticsearch import Elasticsearch
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Elasticsearch configuration
ES_HOST = 'http://localhost:9200'

# Index mappings
INDICES = {
    'retail_association_rules': {
        'mappings': {
            'properties': {
                'rule_id': {'type': 'long'},
                'antecedent': {'type': 'keyword'},
                'consequent': {'type': 'keyword'},
                'antecedent_str': {'type': 'text'},
                'consequent_str': {'type': 'text'},
                'confidence': {'type': 'float'},
                'lift': {'type': 'float'},
                'support': {'type': 'float'}
            }
        },
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }
    },
    
    'retail_demand_forecasts': {
        'mappings': {
            'properties': {
                'StockCode': {'type': 'keyword'},
                'Date': {'type': 'date'},
                'ForecastQuantity': {'type': 'float'},
                'LowerBound': {'type': 'float'},
                'UpperBound': {'type': 'float'}
            }
        },
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }
    },
    
    'retail_safety_stock': {
        'mappings': {
            'properties': {
                'StockCode': {'type': 'keyword'},
                'yhat': {'type': 'float'},
                'yhat_upper': {'type': 'float'},
                'yhat_lower': {'type': 'float'},
                'std_demand': {'type': 'float'},
                'safety_stock': {'type': 'float'},
                'reorder_point': {'type': 'float'}
            }
        },
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }
    },
    
    'retail_realtime_revenue': {
        'mappings': {
            'properties': {
                'window': {
                    'properties': {
                        'start': {'type': 'date'},
                        'end': {'type': 'date'}
                    }
                },
                'WindowStart': {'type': 'date'},
                'WindowEnd': {'type': 'date'},
                'Country': {'type': 'keyword'},
                'TotalRevenue': {'type': 'float'},
                'TransactionCount': {'type': 'long'},
                'UniqueCustomers': {'type': 'long'},
                'AvgOrderValue': {'type': 'float'},
                'TotalQuantity': {'type': 'long'},
                'RevenuePerCustomer': {'type': 'float'}
            }
        },
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'refresh_interval': '5s'
        }
    },
    
    'retail_realtime_products': {
        'mappings': {
            'properties': {
                'window': {
                    'properties': {
                        'start': {'type': 'date'},
                        'end': {'type': 'date'}
                    }
                },
                'WindowStart': {'type': 'date'},
                'WindowEnd': {'type': 'date'},
                'StockCode': {'type': 'keyword'},
                'Description': {'type': 'text'},
                'TotalQuantity': {'type': 'long'},
                'TotalRevenue': {'type': 'float'},
                'TransactionCount': {'type': 'long'},
                'AvgPrice': {'type': 'float'},
                'UniqueCustomers': {'type': 'long'},
                'RevenuePerTransaction': {'type': 'float'}
            }
        },
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'refresh_interval': '5s'
        }
    },
    
    'retail_inventory_alerts': {
        'mappings': {
            'properties': {
                'WindowStart': {'type': 'date'},
                'WindowEnd': {'type': 'date'},
                'StockCode': {'type': 'keyword'},
                'Description': {'type': 'text'},
                'TotalQuantity': {'type': 'long'},
                'CurrentQuantity': {'type': 'long'},
                'AvgQuantity': {'type': 'float'},
                'StdQuantity': {'type': 'float'},
                'AlertType': {'type': 'keyword'},
                'Message': {'type': 'text'},
                'AlertTime': {'type': 'date'},
                'DemandIncreasePct': {'type': 'float'}
            }
        },
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'refresh_interval': '5s'
        }
    },
    
    'retail_customer_segments': {
        'mappings': {
            'properties': {
                'CustomerID': {'type': 'keyword'},
                'Recency': {'type': 'integer'},
                'Frequency': {'type': 'integer'},
                'Monetary': {'type': 'float'},
                'R_Score': {'type': 'integer'},
                'F_Score': {'type': 'integer'},
                'M_Score': {'type': 'integer'},
                'RFM_Score': {'type': 'keyword'},
                'Segment': {'type': 'keyword'},
                'Cluster': {'type': 'integer'},
                'ClusterLabel': {'type': 'keyword'}
            }
        },
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }
    }
}


def main():
    """Main function"""
    logger.info("="*60)
    logger.info("Initializing Elasticsearch Indices")
    logger.info("="*60)
    
    # Connect to Elasticsearch
    try:
        es = Elasticsearch([ES_HOST])
        
        if not es.ping():
            logger.error(f"Cannot connect to Elasticsearch at {ES_HOST}")
            logger.error("Please make sure Elasticsearch is running")
            sys.exit(1)
        
        logger.info(f"✓ Connected to Elasticsearch: {ES_HOST}")
        
    except Exception as e:
        logger.error(f"Error connecting to Elasticsearch: {e}")
        sys.exit(1)
    
    # Create indices
    created_count = 0
    skipped_count = 0
    
    for index_name, config in INDICES.items():
        try:
            if es.indices.exists(index=index_name):
                logger.warning(f"⚠ Index '{index_name}' already exists. Skipping...")
                skipped_count += 1
            else:
                es.indices.create(index=index_name, body=config)
                logger.info(f"✓ Created index: {index_name}")
                created_count += 1
                
        except Exception as e:
            logger.error(f"✗ Error creating index '{index_name}': {e}")
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("Initialization Summary")
    logger.info("="*60)
    logger.info(f"Total indices: {len(INDICES)}")
    logger.info(f"Created: {created_count}")
    logger.info(f"Skipped (already exist): {skipped_count}")
    logger.info("="*60)
    
    # List all indices
    logger.info("\nCurrent Indices:")
    indices = es.cat.indices(format='json')
    for idx in indices:
        if idx['index'].startswith('retail_'):
            logger.info(f"  - {idx['index']}: {idx['docs.count']} docs, {idx['store.size']}")
    
    logger.info("\n✓ Elasticsearch initialization complete!")


if __name__ == "__main__":
    main()
