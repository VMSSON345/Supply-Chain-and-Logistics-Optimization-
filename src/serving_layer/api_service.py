"""
REST API Service for Dashboard
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
from elasticsearch_client import ElasticsearchClient
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Initialize ES client
es_client = ElasticsearchClient()


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'elasticsearch': es_client.client.ping()
    })


@app.route('/api/realtime/revenue', methods=['GET'])
def get_realtime_revenue():
    """Get real-time revenue metrics"""
    try:
        minutes = request.args.get('minutes', 15, type=int)
        
        query = {
            "query": {
                "range": {
                    "window.start": {
                        "gte": f"now-{minutes}m"
                    }
                }
            },
            "sort": [{"window.start": {"order": "desc"}}],
            "size": 1000
        }
        
        df = es_client.search_to_dataframe('retail_realtime_revenue', query)
        
        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
        
    except Exception as e:
        logger.error(f"Error fetching revenue data: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/realtime/products', methods=['GET'])
def get_top_products():
    """Get top products"""
    try:
        limit = request.args.get('limit', 10, type=int)
        
        query = {
            "query": {
                "range": {
                    "window.start": {
                        "gte": "now-10m"
                    }
                }
            },
            "sort": [{"TotalRevenue": {"order": "desc"}}],
            "size": limit
        }
        
        df = es_client.search_to_dataframe('retail_realtime_products', query)
        
        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
        
    except Exception as e:
        logger.error(f"Error fetching products data: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/forecast/product/<stock_code>', methods=['GET'])
def get_product_forecast(stock_code):
    """Get forecast for specific product"""
    try:
        query = {
            "query": {
                "term": {
                    "StockCode": stock_code
                }
            },
            "sort": [{"Date": {"order": "asc"}}],
            "size": 1000
        }
        
        df = es_client.search_to_dataframe('retail_demand_forecasts', query)
        
        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
        
    except Exception as e:
        logger.error(f"Error fetching forecast: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/association/rules', methods=['GET'])
def get_association_rules():
    """Get association rules"""
    try:
        min_confidence = request.args.get('min_confidence', 0.3, type=float)
        min_lift = request.args.get('min_lift', 1.0, type=float)
        limit = request.args.get('limit', 100, type=int)
        
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"confidence": {"gte": min_confidence}}},
                        {"range": {"lift": {"gte": min_lift}}}
                    ]
                }
            },
            "sort": [{"lift": {"order": "desc"}}],
            "size": limit
        }
        
        df = es_client.search_to_dataframe('retail_association_rules', query)
        
        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
        
    except Exception as e:
        logger.error(f"Error fetching association rules: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/alerts/recent', methods=['GET'])
def get_recent_alerts():
    """Get recent alerts"""
    try:
        hours = request.args.get('hours', 1, type=int)
        
        query = {
            "query": {
                "range": {
                    "AlertTime": {
                        "gte": f"now-{hours}h"
                    }
                }
            },
            "sort": [{"AlertTime": {"order": "desc"}}],
            "size": 100
        }
        
        df = es_client.search_to_dataframe('retail_inventory_alerts', query)
        
        return jsonify({
            'success': True,
            'data': df.to_dict('records'),
            'count': len(df)
        })
        
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/segments', methods=['GET'])
def get_customer_segments():
    """Get customer segmentation"""
    try:
        query = {
            "query": {"match_all": {}},
            "size": 10000
        }
        
        df = es_client.search_to_dataframe('retail_customer_segments', query)
        
        # Aggregate by segment
        segment_counts = df.groupby('ClusterLabel').size().to_dict()
        
        return jsonify({
            'success': True,
            'segment_distribution': segment_counts,
            'total_customers': len(df)
        })
        
    except Exception as e:
        logger.error(f"Error fetching segments: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/stats/overview', methods=['GET'])
def get_overview_stats():
    """Get overview statistics"""
    try:
        stats = {
            'indices': {},
            'timestamp': datetime.now().isoformat()
        }
        
        indices = [
            'retail_realtime_revenue',
            'retail_realtime_products',
            'retail_demand_forecasts',
            'retail_association_rules',
            'retail_inventory_alerts',
            'retail_customer_segments'
        ]
        
        for index in indices:
            try:
                index_stats = es_client.get_index_stats(index)
                stats['indices'][index] = index_stats
            except:
                stats['indices'][index] = {'error': 'Index not found'}
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
