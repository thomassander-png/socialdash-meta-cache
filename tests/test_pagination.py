"""
Tests for pagination helper functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.meta_client import MetaClient
from src.config import Config


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    return Config(
        meta_access_token="test_token_12345",
        meta_api_version="v20.0",
        fb_page_ids=["123456789"],
        database_url="postgresql://test:test@localhost/test",
        timezone="Europe/Berlin"
    )


@pytest.fixture
def meta_client(mock_config):
    """Create a MetaClient instance with mock config."""
    return MetaClient(mock_config)


class TestPagination:
    """Test cases for pagination functionality."""
    
    def test_single_page_response(self, meta_client):
        """Test handling of single page response (no pagination needed)."""
        mock_response = {
            "data": [
                {"id": "post_1", "message": "Test 1"},
                {"id": "post_2", "message": "Test 2"},
            ],
            "paging": {}
        }
        
        with patch.object(meta_client, '_make_request', return_value=mock_response):
            results = list(meta_client._paginate("test_endpoint"))
        
        assert len(results) == 2
        assert results[0]["id"] == "post_1"
        assert results[1]["id"] == "post_2"
    
    def test_multi_page_response(self, meta_client):
        """Test handling of multi-page response with cursor pagination."""
        page1_response = {
            "data": [
                {"id": "post_1", "message": "Test 1"},
                {"id": "post_2", "message": "Test 2"},
            ],
            "paging": {
                "cursors": {"after": "cursor_abc"},
                "next": "https://graph.facebook.com/v20.0/test?after=cursor_abc"
            }
        }
        
        page2_response = {
            "data": [
                {"id": "post_3", "message": "Test 3"},
            ],
            "paging": {}
        }
        
        with patch.object(meta_client, '_make_request') as mock_request:
            mock_request.side_effect = [page1_response, page2_response]
            results = list(meta_client._paginate("test_endpoint"))
        
        assert len(results) == 3
        assert results[0]["id"] == "post_1"
        assert results[2]["id"] == "post_3"
        assert mock_request.call_count == 2
    
    def test_empty_response(self, meta_client):
        """Test handling of empty response."""
        mock_response = {
            "data": [],
            "paging": {}
        }
        
        with patch.object(meta_client, '_make_request', return_value=mock_response):
            results = list(meta_client._paginate("test_endpoint"))
        
        assert len(results) == 0
    
    def test_pagination_stops_on_no_next(self, meta_client):
        """Test that pagination stops when there's no next cursor."""
        mock_response = {
            "data": [{"id": "post_1"}],
            "paging": {
                "cursors": {"before": "cursor_xyz"}
                # No "after" cursor means no more pages
            }
        }
        
        with patch.object(meta_client, '_make_request', return_value=mock_response):
            results = list(meta_client._paginate("test_endpoint"))
        
        assert len(results) == 1
    
    def test_pagination_limit_parameter(self, meta_client):
        """Test that limit parameter is passed correctly."""
        mock_response = {"data": [], "paging": {}}
        
        with patch.object(meta_client, '_make_request', return_value=mock_response) as mock_request:
            list(meta_client._paginate("test_endpoint", limit=50))
        
        # Check that limit was passed in params
        call_args = mock_request.call_args
        assert call_args[0][1]["limit"] == 50


class TestMaskedToken:
    """Test token masking for security."""
    
    def test_token_masking_long(self, mock_config):
        """Test that long tokens are properly masked."""
        mock_config.meta_access_token = "EAABsbCS1234567890abcdefghijklmnop"
        masked = mock_config.get_masked_token()
        
        assert masked == "EAAB...mnop"
        assert "1234567890" not in masked
    
    def test_token_masking_short(self, mock_config):
        """Test that short tokens are fully masked."""
        mock_config.meta_access_token = "short"
        masked = mock_config.get_masked_token()
        
        assert masked == "****"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
