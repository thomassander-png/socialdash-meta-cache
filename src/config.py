"""
Configuration module for SocialDash Meta Cache.
Loads settings from environment variables.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Config:
    """Application configuration loaded from environment variables."""
    
    # Meta API Settings
    meta_access_token: str = ""
    meta_api_version: str = "v20.0"
    fb_page_ids: List[str] = field(default_factory=list)
    ig_account_ids: List[str] = field(default_factory=list)
    
    # Database
    database_url: str = ""
    
    # Timezone
    timezone: str = "Europe/Berlin"
    
    # Optional Meta App credentials (for token refresh)
    meta_app_id: Optional[str] = None
    meta_app_secret: Optional[str] = None
    
    # Cache settings
    lookback_days: int = 45
    
    # Preview caching
    preview_cache_enabled: bool = False
    supabase_storage_url: Optional[str] = None
    supabase_service_role_key: Optional[str] = None
    
    def __init__(self):
        """Load configuration from environment variables."""
        self.meta_access_token = os.environ.get("META_ACCESS_TOKEN", "")
        self.meta_api_version = os.environ.get("META_API_VERSION", "v20.0")
        self.database_url = os.environ.get("DATABASE_URL", "")
        self.timezone = os.environ.get("TZ", "Europe/Berlin")
        self.meta_app_id = os.environ.get("META_APP_ID")
        self.meta_app_secret = os.environ.get("META_APP_SECRET")
        self.lookback_days = int(os.environ.get("LOOKBACK_DAYS", "45"))
        
        # Parse Facebook Page IDs
        fb_page_ids_raw = os.environ.get("FB_PAGE_IDS", "")
        self.fb_page_ids = [pid.strip() for pid in fb_page_ids_raw.split(",") if pid.strip()]
        
        # Parse Instagram Account IDs
        ig_account_ids_raw = os.environ.get("IG_ACCOUNT_IDS", "")
        self.ig_account_ids = [aid.strip() for aid in ig_account_ids_raw.split(",") if aid.strip()]
        
        # Preview caching settings
        self.preview_cache_enabled = os.environ.get("PREVIEW_CACHE_ENABLED", "false").lower() == "true"
        self.supabase_storage_url = os.environ.get("SUPABASE_STORAGE_URL")
        self.supabase_service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    def validate(self, require_fb: bool = True, require_ig: bool = False):
        """Validate required configuration."""
        if not self.meta_access_token:
            raise ValueError("META_ACCESS_TOKEN environment variable is required")
        
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        if require_fb and not self.fb_page_ids:
            raise ValueError("FB_PAGE_IDS environment variable is required (comma-separated)")
        
        if require_ig and not self.ig_account_ids:
            raise ValueError("IG_ACCOUNT_IDS environment variable is required (comma-separated)")
    
    def get_fb_page_ids(self) -> List[str]:
        """Get Facebook Page IDs."""
        return self.fb_page_ids
    
    def get_ig_account_ids(self) -> List[str]:
        """Get Instagram Account IDs."""
        return self.ig_account_ids
    
    def get_masked_token(self) -> str:
        """Return masked token for logging (first 4 and last 4 chars)."""
        if len(self.meta_access_token) <= 12:
            return "****"
        return f"{self.meta_access_token[:4]}...{self.meta_access_token[-4:]}"


def get_config() -> Config:
    """Get application configuration singleton."""
    return Config()
