"""
Configuration module for SocialDash Meta Cache.
Loads settings from environment variables.
"""

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Config:
    """Application configuration loaded from environment variables."""
    
    # Meta API Settings
    meta_access_token: str
    meta_api_version: str
    fb_page_ids: List[str]
    
    # Database
    database_url: str
    
    # Timezone
    timezone: str
    
    # Optional Meta App credentials (for token refresh)
    meta_app_id: Optional[str] = None
    meta_app_secret: Optional[str] = None
    
    # Cache settings
    lookback_days: int = 45
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        
        meta_access_token = os.environ.get("META_ACCESS_TOKEN")
        if not meta_access_token:
            raise ValueError("META_ACCESS_TOKEN environment variable is required")
        
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        fb_page_ids_raw = os.environ.get("FB_PAGE_IDS", "")
        fb_page_ids = [pid.strip() for pid in fb_page_ids_raw.split(",") if pid.strip()]
        if not fb_page_ids:
            raise ValueError("FB_PAGE_IDS environment variable is required (comma-separated)")
        
        return cls(
            meta_access_token=meta_access_token,
            meta_api_version=os.environ.get("META_API_VERSION", "v20.0"),
            fb_page_ids=fb_page_ids,
            database_url=database_url,
            timezone=os.environ.get("TZ", "Europe/Berlin"),
            meta_app_id=os.environ.get("META_APP_ID"),
            meta_app_secret=os.environ.get("META_APP_SECRET"),
            lookback_days=int(os.environ.get("LOOKBACK_DAYS", "45")),
        )
    
    def get_masked_token(self) -> str:
        """Return masked token for logging (first 4 and last 4 chars)."""
        if len(self.meta_access_token) <= 12:
            return "****"
        return f"{self.meta_access_token[:4]}...{self.meta_access_token[-4:]}"


def get_config() -> Config:
    """Get application configuration singleton."""
    return Config.from_env()
