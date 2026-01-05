"""
Meta/Facebook Graph API client with retry logic, pagination, and rate limit handling.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Config

logger = logging.getLogger(__name__)


class MetaAPIError(Exception):
    """Custom exception for Meta API errors."""
    
    def __init__(self, message: str, error_code: Optional[int] = None, error_subcode: Optional[int] = None):
        super().__init__(message)
        self.error_code = error_code
        self.error_subcode = error_subcode


class MetaClient:
    """Client for interacting with Meta/Facebook Graph API."""
    
    BASE_URL = "https://graph.facebook.com"
    
    # Rate limit settings
    MAX_RETRIES = 5
    INITIAL_BACKOFF = 1  # seconds
    MAX_BACKOFF = 60  # seconds
    
    def __init__(self, config: Config):
        self.config = config
        self.api_version = config.meta_api_version
        self.access_token = config.meta_access_token
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def _build_url(self, endpoint: str) -> str:
        """Build full API URL."""
        return f"{self.BASE_URL}/{self.api_version}/{endpoint}"
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> Dict[str, Any]:
        """
        Make an API request with exponential backoff for rate limits.
        """
        url = self._build_url(endpoint)
        params = params or {}
        params["access_token"] = self.access_token
        
        backoff = self.INITIAL_BACKOFF
        
        for attempt in range(self.MAX_RETRIES):
            try:
                if method == "GET":
                    response = self.session.get(url, params=params, timeout=30)
                else:
                    response = self.session.post(url, data=params, timeout=30)
                
                # Check for rate limiting
                if response.status_code == 429:
                    logger.warning(f"Rate limited. Waiting {backoff}s before retry...")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.MAX_BACKOFF)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                # Parse Meta API error response
                try:
                    error_data = e.response.json().get("error", {})
                    error_code = error_data.get("code")
                    error_subcode = error_data.get("error_subcode")
                    error_message = error_data.get("message", str(e))
                    
                    # Rate limit errors
                    if error_code in [4, 17, 32, 613]:
                        logger.warning(f"Rate limit error (code {error_code}). Waiting {backoff}s...")
                        time.sleep(backoff)
                        backoff = min(backoff * 2, self.MAX_BACKOFF)
                        continue
                    
                    raise MetaAPIError(error_message, error_code, error_subcode)
                    
                except (ValueError, KeyError):
                    raise MetaAPIError(str(e))
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.MAX_BACKOFF)
                else:
                    raise MetaAPIError(f"Request failed after {self.MAX_RETRIES} attempts: {e}")
        
        raise MetaAPIError("Max retries exceeded")
    
    def _paginate(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 100
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Paginate through API results.
        Yields individual items from the 'data' array.
        """
        params = params or {}
        params["limit"] = limit
        
        while True:
            response = self._make_request(endpoint, params)
            
            data = response.get("data", [])
            for item in data:
                yield item
            
            # Check for next page
            paging = response.get("paging", {})
            next_url = paging.get("next")
            
            if not next_url or not data:
                break
            
            # Extract cursor for next request
            cursors = paging.get("cursors", {})
            after = cursors.get("after")
            
            if after:
                params["after"] = after
            else:
                break
    
    def get(self, endpoint: str, fields: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Generic GET request to the Graph API.
        
        Args:
            endpoint: API endpoint (e.g., '/me/accounts' or '/{page_id}')
            fields: Comma-separated list of fields to request
            **kwargs: Additional parameters to pass to the API
            
        Returns:
            API response as dict
        """
        # Remove leading slash if present
        endpoint = endpoint.lstrip('/')
        
        params = dict(kwargs)
        if fields:
            params['fields'] = fields
            
        return self._make_request(endpoint, params)
    
    def get_paginated(self, endpoint: str, fields: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        """
        GET request with automatic pagination.
        Returns all results from all pages.
        
        Args:
            endpoint: API endpoint
            fields: Comma-separated list of fields to request
            **kwargs: Additional parameters
            
        Returns:
            List of all items from all pages
        """
        endpoint = endpoint.lstrip('/')
        
        params = dict(kwargs)
        if fields:
            params['fields'] = fields
        
        all_items = []
        for item in self._paginate(endpoint, params):
            all_items.append(item)
        
        return all_items
    
    def request_url(self, url: str) -> Dict[str, Any]:
        """
        Make a request to a full URL (for pagination next links).
        
        Args:
            url: Full URL to request
            
        Returns:
            API response as dict
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise MetaAPIError(f"Request to {url} failed: {e}")

    def get_page_info(self, page_id: str) -> Dict[str, Any]:
        """Get basic page information."""
        params = {"fields": "id,name,fan_count,followers_count"}
        return self._make_request(page_id, params)
    
    def get_page_posts(
        self,
        page_id: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        lookback_days: int = 45
    ) -> List[Dict[str, Any]]:
        """
        Get posts from a page.
        
        Note: since/until parameters can be unreliable with the Graph API,
        so we fetch all recent posts and filter client-side.
        """
        # Note: 'type' and 'shares' fields are deprecated in v3.3+
        # Use 'attachments' to determine post type instead
        params = {
            "fields": "id,created_time,message,permalink_url,attachments{type,media_type}"
        }
        
        # Calculate date boundaries
        if since is None:
            since = datetime.utcnow() - timedelta(days=lookback_days)
        if until is None:
            until = datetime.utcnow()
        
        posts = []
        
        for post in self._paginate(f"{page_id}/posts", params):
            # Parse created_time
            created_time_str = post.get("created_time")
            if created_time_str:
                created_time = datetime.fromisoformat(created_time_str.replace("Z", "+00:00"))
                
                # Client-side date filtering (more reliable than API params)
                if created_time.replace(tzinfo=None) < since.replace(tzinfo=None) if hasattr(since, 'replace') else since:
                    # Posts are returned in reverse chronological order
                    # Once we hit posts older than 'since', we can stop
                    break
                
                if created_time.replace(tzinfo=None) <= until.replace(tzinfo=None) if hasattr(until, 'replace') else until:
                    posts.append(post)
        
        logger.info(f"Fetched {len(posts)} posts from page {page_id}")
        return posts
    
    def get_post_insights(self, post_id: str) -> Dict[str, Any]:
        """
        Get insights/metrics for a specific post.
        
        Note: Not all metrics are available for all post types.
        Some metrics require page-level permissions.
        """
        metrics = [
            "post_impressions",
            "post_impressions_unique",  # reach
            "post_engaged_users",
            "post_clicks",
            "post_reactions_by_type_total",
            "post_video_views",  # 3s views for videos
        ]
        
        params = {
            "metric": ",".join(metrics)
        }
        
        try:
            response = self._make_request(f"{post_id}/insights", params)
            return self._parse_insights(response)
        except MetaAPIError as e:
            # Some posts may not have insights available
            logger.warning(f"Could not fetch insights for post {post_id}: {e}")
            return {}
    
    def _parse_insights(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parse insights response into a flat dictionary."""
        insights = {}
        
        for item in response.get("data", []):
            name = item.get("name")
            values = item.get("values", [])
            
            if values:
                value = values[0].get("value")
                
                # Handle reactions breakdown
                if name == "post_reactions_by_type_total" and isinstance(value, dict):
                    insights["reactions_breakdown"] = value
                    insights["reactions_total"] = sum(value.values())
                else:
                    insights[name] = value
        
        return insights
    
    def get_post_reactions_count(self, post_id: str) -> int:
        """Get total reactions count for a post."""
        params = {"summary": "true"}
        
        try:
            response = self._make_request(f"{post_id}/reactions", params)
            return response.get("summary", {}).get("total_count", 0)
        except MetaAPIError:
            return 0
    
    def get_post_comments_count(self, post_id: str) -> int:
        """Get total comments count for a post."""
        params = {"summary": "true"}
        
        try:
            response = self._make_request(f"{post_id}/comments", params)
            return response.get("summary", {}).get("total_count", 0)
        except MetaAPIError:
            return 0
    
    def get(self, endpoint: str, fields: str = None, **kwargs) -> Dict[str, Any]:
        """
        Simple GET request to the Graph API.
        
        Args:
            endpoint: API endpoint (e.g., '/12345' or '12345')
            fields: Comma-separated list of fields to retrieve
            **kwargs: Additional query parameters
        
        Returns:
            API response as dictionary
        """
        # Clean endpoint - remove leading slash if present
        endpoint = endpoint.lstrip('/')
        
        params = dict(kwargs)
        if fields:
            params['fields'] = fields
        
        try:
            return self._make_request(endpoint, params)
        except Exception as e:
            logger.error(f"GET request failed for {endpoint}: {e}")
            return {}
    
    def get_paginated(self, endpoint: str, fields: str = None, limit: int = 100, **kwargs) -> List[Dict[str, Any]]:
        """
        GET request with pagination - returns all results as a list.
        
        Args:
            endpoint: API endpoint
            fields: Comma-separated list of fields to retrieve
            limit: Number of items per page
            **kwargs: Additional query parameters
        
        Returns:
            List of all items from paginated response
        """
        # Clean endpoint - remove leading slash if present
        endpoint = endpoint.lstrip('/')
        
        params = dict(kwargs)
        if fields:
            params['fields'] = fields
        
        results = []
        try:
            for item in self._paginate(endpoint, params, limit):
                results.append(item)
        except Exception as e:
            logger.error(f"Paginated GET request failed for {endpoint}: {e}")
        
        return results

    def get_post_full_metrics(self, post_id: str, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get all available metrics for a post.
        Combines edge counts with insights where available.
        """
        metrics = {
            "reactions_total": 0,
            "comments_total": 0,
            "shares_total": None,
            "shares_limited": True,
            "reach": None,
            "impressions": None,
            "video_3s_views": None,
        }
        
        # Get reactions count
        metrics["reactions_total"] = self.get_post_reactions_count(post_id)
        
        # Get comments count
        metrics["comments_total"] = self.get_post_comments_count(post_id)
        
        # Get shares from post data if available
        shares_data = post_data.get("shares", {})
        if shares_data:
            metrics["shares_total"] = shares_data.get("count")
            metrics["shares_limited"] = False
        
        # Try to get insights (may not be available for all posts)
        insights = self.get_post_insights(post_id)
        
        if insights:
            # Override with insights data if available
            if "reactions_total" in insights:
                metrics["reactions_total"] = insights["reactions_total"]
            
            metrics["reach"] = insights.get("post_impressions_unique")
            metrics["impressions"] = insights.get("post_impressions")
            metrics["video_3s_views"] = insights.get("post_video_views")
        
        return metrics
