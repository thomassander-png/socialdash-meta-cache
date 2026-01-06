"""
Meta/Facebook Graph API client with retry logic, pagination, and rate limit handling.
Supports Page Access Tokens for Facebook Page API calls.
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
        
        # Page Access Tokens cache - maps page_id to page_access_token
        self._page_tokens: Dict[str, str] = {}
        self._page_tokens_loaded = False
    
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
    
    def _load_page_tokens(self):
        """Load Page Access Tokens from /me/accounts endpoint."""
        if self._page_tokens_loaded:
            return
        
        logger.info("Loading Page Access Tokens...")
        
        try:
            url = f"{self.BASE_URL}/{self.api_version}/me/accounts"
            params = {
                "access_token": self.access_token,
                "fields": "id,name,access_token",
                "limit": 100
            }
            
            while True:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                for page in data.get("data", []):
                    page_id = page.get("id")
                    page_token = page.get("access_token")
                    page_name = page.get("name", "Unknown")
                    
                    if page_id and page_token:
                        self._page_tokens[page_id] = page_token
                        logger.info(f"Loaded token for page: {page_name} ({page_id})")
                
                # Check for next page
                paging = data.get("paging", {})
                next_url = paging.get("next")
                
                if not next_url:
                    break
                
                # Use next URL directly
                url = next_url
                params = {}  # Next URL already has params
            
            logger.info(f"Loaded {len(self._page_tokens)} Page Access Tokens")
            self._page_tokens_loaded = True
            
        except Exception as e:
            logger.error(f"Failed to load Page Access Tokens: {e}")
            # Continue without page tokens - will use system user token as fallback
            self._page_tokens_loaded = True
    
    def get_page_token(self, page_id: str) -> str:
        """Get the Page Access Token for a specific page."""
        if not self._page_tokens_loaded:
            self._load_page_tokens()
        
        return self._page_tokens.get(page_id, self.access_token)
    
    def _build_url(self, endpoint: str) -> str:
        """Build full API URL."""
        return f"{self.BASE_URL}/{self.api_version}/{endpoint}"
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        use_page_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Make an API request with exponential backoff for rate limits.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            method: HTTP method
            use_page_token: If provided, use the Page Access Token for this page_id
        """
        url = self._build_url(endpoint)
        params = params or {}
        
        # Use Page Access Token if specified, otherwise use system user token
        if use_page_token:
            params["access_token"] = self.get_page_token(use_page_token)
        else:
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
        limit: int = 100,
        use_page_token: Optional[str] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Paginate through API results.
        Yields individual items from the 'data' array.
        """
        params = params or {}
        params["limit"] = limit
        
        while True:
            response = self._make_request(endpoint, params, use_page_token=use_page_token)
            
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
    
    def get(self, endpoint: str, fields: Optional[str] = None, use_page_token: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Generic GET request to the Graph API.
        
        Args:
            endpoint: API endpoint (e.g., '/me/accounts' or '/{page_id}')
            fields: Comma-separated list of fields to request
            use_page_token: If provided, use the Page Access Token for this page_id
            **kwargs: Additional parameters to pass to the API
            
        Returns:
            API response as dict
        """
        # Remove leading slash if present
        endpoint = endpoint.lstrip('/')
        
        params = dict(kwargs)
        if fields:
            params['fields'] = fields
            
        return self._make_request(endpoint, params, use_page_token=use_page_token)
    
    def get_paginated(self, endpoint: str, fields: Optional[str] = None, use_page_token: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        """
        GET request with automatic pagination.
        Returns all results from all pages.
        
        Args:
            endpoint: API endpoint
            fields: Comma-separated list of fields to request
            use_page_token: If provided, use the Page Access Token for this page_id
            **kwargs: Additional parameters
            
        Returns:
            List of all items from all pages
        """
        endpoint = endpoint.lstrip('/')
        
        params = dict(kwargs)
        if fields:
            params['fields'] = fields
        
        all_items = []
        for item in self._paginate(endpoint, params, use_page_token=use_page_token):
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
        return self._make_request(page_id, params, use_page_token=page_id)
    
    def get_page_posts(
        self,
        page_id: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        lookback_days: int = 45
    ) -> List[Dict[str, Any]]:
        """
        Get posts from a page.
        Uses Page Access Token for the specific page.
        
        Note: since/until parameters can be unreliable with the Graph API,
        so we fetch all recent posts and filter client-side.
        """
        # Ensure page tokens are loaded
        if not self._page_tokens_loaded:
            self._load_page_tokens()
        
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
        
        # Use Page Access Token for this page
        for post in self._paginate(f"{page_id}/feed", params, use_page_token=page_id):
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
    
    def get_post_insights(self, post_id: str, page_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get insights/metrics for a specific post.
        Uses Page Access Token if page_id is provided.
        
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
        
        # Extract page_id from post_id if not provided (format: page_id_post_id)
        if page_id is None and "_" in post_id:
            page_id = post_id.split("_")[0]
        
        try:
            response = self._make_request(f"{post_id}/insights", params, use_page_token=page_id)
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
    
    def get_post_reactions_count(self, post_id: str, page_id: Optional[str] = None) -> int:
        """Get total reactions count for a post."""
        params = {"summary": "true"}
        
        # Extract page_id from post_id if not provided
        if page_id is None and "_" in post_id:
            page_id = post_id.split("_")[0]
        
        try:
            response = self._make_request(f"{post_id}/reactions", params, use_page_token=page_id)
            return response.get("summary", {}).get("total_count", 0)
        except MetaAPIError:
            return 0
    
    def get_post_comments_count(self, post_id: str, page_id: Optional[str] = None) -> int:
        """Get total comments count for a post."""
        params = {"summary": "true"}
        
        # Extract page_id from post_id if not provided
        if page_id is None and "_" in post_id:
            page_id = post_id.split("_")[0]
        
        try:
            response = self._make_request(f"{post_id}/comments", params, use_page_token=page_id)
            return response.get("summary", {}).get("total_count", 0)
        except MetaAPIError:
            return 0

    def get_post_full_metrics(self, post_id: str, post_data: Dict[str, Any], page_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all available metrics for a post.
        Combines edge counts with insights where available.
        Uses Page Access Token if page_id is provided.
        """
        # Extract page_id from post_id if not provided
        if page_id is None and "_" in post_id:
            page_id = post_id.split("_")[0]
        
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
        metrics["reactions_total"] = self.get_post_reactions_count(post_id, page_id)
        
        # Get comments count
        metrics["comments_total"] = self.get_post_comments_count(post_id, page_id)
        
        # Get shares from post data if available
        shares_data = post_data.get("shares", {})
        if shares_data:
            metrics["shares_total"] = shares_data.get("count")
            metrics["shares_limited"] = False
        
        # Try to get insights (may not be available for all posts)
        insights = self.get_post_insights(post_id, page_id)
        
        if insights:
            # Override with insights data if available
            if "reactions_total" in insights:
                metrics["reactions_total"] = insights["reactions_total"]
            
            metrics["reach"] = insights.get("post_impressions_unique")
            metrics["impressions"] = insights.get("post_impressions")
            metrics["video_3s_views"] = insights.get("post_video_views")
        
        return metrics
