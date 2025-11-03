"""
Elasticsearch Client Wrapper
"""
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError, RequestError
import logging
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


class ElasticsearchClient:
    """Wrapper cho Elasticsearch operations"""
    
    def __init__(self, hosts: List[str] = None, **kwargs):
        """
        Initialize ES client
        
        Args:
            hosts: List of ES hosts
            **kwargs: Additional ES client parameters
        """
        self.hosts = hosts or ['http://localhost:9200']
        self.client = Elasticsearch(self.hosts, **kwargs)
        
        # Test connection
        if self.client.ping():
            logger.info(f"✅ Connected to Elasticsearch: {self.hosts}")
        else:
            logger.error(f"❌ Failed to connect to Elasticsearch: {self.hosts}")
    
    def create_index(self, index_name: str, mappings: dict = None, 
                    settings: dict = None):
        """
        Tạo index mới
        
        Args:
            index_name: Tên index
            mappings: Index mappings
            settings: Index settings
        """
        try:
            if self.client.indices.exists(index=index_name):
                logger.warning(f"Index '{index_name}' already exists")
                return False
            
            body = {}
            if mappings:
                body['mappings'] = mappings
            if settings:
                body['settings'] = settings
            
            self.client.indices.create(index=index_name, body=body)
            logger.info(f"✅ Created index: {index_name}")
            return True
            
        except RequestError as e:
            logger.error(f"Error creating index '{index_name}': {e}")
            return False
    
    def delete_index(self, index_name: str):
        """Xóa index"""
        try:
            self.client.indices.delete(index=index_name)
            logger.info(f"✅ Deleted index: {index_name}")
            return True
        except NotFoundError:
            logger.warning(f"Index '{index_name}' not found")
            return False
    
    def index_document(self, index_name: str, doc_id: str, document: dict):
        """
        Index một document
        """
        try:
            response = self.client.index(
                index=index_name,
                id=doc_id,
                document=document
            )
            return response
        except Exception as e:
            logger.error(f"Error indexing document: {e}")
            return None
    
    def bulk_index(self, index_name: str, documents: List[dict], 
                   id_field: str = None):
        """
        Bulk index documents
        
        Args:
            index_name: Index name
            documents: List of documents
            id_field: Field to use as document ID (optional)
        """
        try:
            actions = []
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                
                actions.append(action)
            
            success, failed = helpers.bulk(
                self.client,
                actions,
                raise_on_error=False
            )
            
            logger.info(f"✅ Bulk indexed {success} documents to '{index_name}'")
            
            if failed:
                logger.warning(f"Failed to index {len(failed)} documents")
            
            return success, failed
            
        except Exception as e:
            logger.error(f"Error in bulk indexing: {e}")
            return 0, len(documents)
    
    def bulk_index_dataframe(self, index_name: str, df: pd.DataFrame, 
                            id_field: str = None):
        """
        Bulk index from pandas DataFrame
        """
        documents = df.to_dict('records')
        return self.bulk_index(index_name, documents, id_field)
    
    def search(self, index_name: str, query: dict, size: int = 100) -> dict:
        """
        Search documents
        
        Args:
            index_name: Index name
            query: ES query DSL
            size: Number of results
        """
        try:
            response = self.client.search(
                index=index_name,
                body=query,
                size=size
            )
            return response
        except Exception as e:
            logger.error(f"Error searching index '{index_name}': {e}")
            return {}
    
    def search_to_dataframe(self, index_name: str, query: dict, 
                           size: int = 10000) -> pd.DataFrame:
        """
        Search và convert results to DataFrame
        """
        response = self.search(index_name, query, size)
        
        if 'hits' in response and 'hits' in response['hits']:
            hits = response['hits']['hits']
            data = [hit['_source'] for hit in hits]
            return pd.DataFrame(data)
        
        return pd.DataFrame()
    
    def get_document(self, index_name: str, doc_id: str) -> dict:
        """Get document by ID"""
        try:
            response = self.client.get(index=index_name, id=doc_id)
            return response['_source']
        except NotFoundError:
            logger.warning(f"Document '{doc_id}' not found in '{index_name}'")
            return {}
    
    def update_document(self, index_name: str, doc_id: str, 
                       partial_doc: dict):
        """Update document"""
        try:
            response = self.client.update(
                index=index_name,
                id=doc_id,
                body={"doc": partial_doc}
            )
            return response
        except Exception as e:
            logger.error(f"Error updating document: {e}")
            return None
    
    def delete_document(self, index_name: str, doc_id: str):
        """Delete document"""
        try:
            response = self.client.delete(index=index_name, id=doc_id)
            return response
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            return None
    
    def count(self, index_name: str, query: dict = None) -> int:
        """Count documents"""
        try:
            body = {"query": query} if query else {}
            response = self.client.count(index=index_name, body=body)
            return response['count']
        except Exception as e:
            logger.error(f"Error counting documents: {e}")
            return 0
    
    def refresh_index(self, index_name: str):
        """Refresh index"""
        try:
            self.client.indices.refresh(index=index_name)
            logger.info(f"✅ Refreshed index: {index_name}")
        except Exception as e:
            logger.error(f"Error refreshing index: {e}")
    
    def get_index_stats(self, index_name: str) -> dict:
        """Get index statistics"""
        try:
            stats = self.client.indices.stats(index=index_name)
            
            index_stats = stats['indices'][index_name]
            
            return {
                'total_docs': index_stats['total']['docs']['count'],
                'deleted_docs': index_stats['total']['docs']['deleted'],
                'size_in_bytes': index_stats['total']['store']['size_in_bytes'],
                'size_mb': round(index_stats['total']['store']['size_in_bytes'] / (1024**2), 2)
            }
        except Exception as e:
            logger.error(f"Error getting index stats: {e}")
            return {}
    
    def list_indices(self) -> List[str]:
        """List all indices"""
        try:
            indices = self.client.cat.indices(format='json')
            return [idx['index'] for idx in indices]
        except Exception as e:
            logger.error(f"Error listing indices: {e}")
            return []
    
    def close(self):
        """Close connection"""
        self.client.close()
        logger.info("Elasticsearch connection closed")
