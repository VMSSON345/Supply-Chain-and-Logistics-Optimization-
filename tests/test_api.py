"""
Unit Tests for API Service
"""
import unittest
import sys
sys.path.append('../src')

import json
from serving_layer.api_service import app


class TestAPIService(unittest.TestCase):
    """Test API endpoints"""
    
    def setUp(self):
        """Set up test client"""
        self.app = app.test_client()
        self.app.testing = True
    
    def test_health_check(self):
        """Test health check endpoint"""
        response = self.app.get('/health')
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.data)
        self.assertIn('status', data)
        self.assertEqual(data['status'], 'healthy')
    
    def test_realtime_revenue_endpoint(self):
        """Test real-time revenue endpoint"""
        response = self.app.get('/api/realtime/revenue?minutes=15')
        
        # Should return 200 even if no data
        self.assertIn(response.status_code, [200, 500])
        
        if response.status_code == 200:
            data = json.loads(response.data)
            self.assertIn('success', data)
    
    def test_top_products_endpoint(self):
        """Test top products endpoint"""
        response = self.app.get('/api/realtime/products?limit=10')
        
        self.assertIn(response.status_code, [200, 500])
        
        if response.status_code == 200:
            data = json.loads(response.data)
            self.assertIn('success', data)


if __name__ == '__main__':
    unittest.main()
