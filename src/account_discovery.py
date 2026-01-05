"""
Module for automatic discovery of Facebook Pages and Instagram Business Accounts.
Discovers all accounts accessible via the Meta API token and registers them in the database.
"""

import logging
from typing import Dict, List, Optional, Any

from .config import Config
from .db import Database
from .meta_client import MetaClient, MetaAPIError

logger = logging.getLogger(__name__)


class AccountDiscovery:
    """Handles automatic discovery of FB Pages and IG Business Accounts."""
    
    def __init__(self, config: Config, db: Database, client: MetaClient):
        self.config = config
        self.db = db
        self.client = client
    
    def discover_all_accounts(self) -> Dict[str, Any]:
        """
        Discover all Facebook Pages and Instagram Business Accounts.
        
        Returns:
            Dict with discovery results
        """
        results = {
            "fb_pages": [],
            "ig_accounts": [],
            "customer_accounts_created": 0,
            "errors": []
        }
        
        # Step 1: Discover Facebook Pages
        logger.info("Discovering Facebook Pages...")
        try:
            fb_pages = self._discover_fb_pages()
            results["fb_pages"] = fb_pages
            logger.info(f"Discovered {len(fb_pages)} Facebook Pages")
        except Exception as e:
            logger.error(f"Failed to discover FB Pages: {e}")
            results["errors"].append(f"FB Pages: {str(e)}")
        
        # Step 2: Discover Instagram Business Accounts
        logger.info("Discovering Instagram Business Accounts...")
        try:
            ig_accounts = self._discover_ig_accounts(results["fb_pages"])
            results["ig_accounts"] = ig_accounts
            logger.info(f"Discovered {len(ig_accounts)} Instagram Business Accounts")
        except Exception as e:
            logger.error(f"Failed to discover IG Accounts: {e}")
            results["errors"].append(f"IG Accounts: {str(e)}")
        
        # Step 3: Create customer_accounts entries
        logger.info("Creating customer_accounts entries...")
        created = self._create_customer_accounts(
            results["fb_pages"], 
            results["ig_accounts"]
        )
        results["customer_accounts_created"] = created
        
        return results
    
    def _discover_fb_pages(self) -> List[Dict[str, Any]]:
        """
        Discover all Facebook Pages accessible via /me/accounts.
        
        Returns:
            List of page data dicts
        """
        pages = []
        
        try:
            # Get all pages via /me/accounts
            response = self.client.get(
                "/me/accounts",
                fields="id,name,access_token,category,tasks"
            )
            
            if not response or "data" not in response:
                logger.warning("No pages found in /me/accounts response")
                return pages
            
            for page_data in response.get("data", []):
                page_id = page_data.get("id")
                page_name = page_data.get("name", f"Page {page_id}")
                
                if not page_id:
                    continue
                
                # Upsert to fb_pages table
                self.db.upsert_page(page_id=page_id, name=page_name)
                
                pages.append({
                    "page_id": page_id,
                    "name": page_name,
                    "category": page_data.get("category"),
                    "has_access_token": "access_token" in page_data
                })
                
                logger.info(f"Discovered FB Page: {page_name} ({page_id})")
            
            # Handle pagination
            while "paging" in response and "next" in response["paging"]:
                next_url = response["paging"]["next"]
                response = self.client.request_url(next_url)
                
                for page_data in response.get("data", []):
                    page_id = page_data.get("id")
                    page_name = page_data.get("name", f"Page {page_id}")
                    
                    if not page_id:
                        continue
                    
                    self.db.upsert_page(page_id=page_id, name=page_name)
                    
                    pages.append({
                        "page_id": page_id,
                        "name": page_name,
                        "category": page_data.get("category"),
                        "has_access_token": "access_token" in page_data
                    })
                    
                    logger.info(f"Discovered FB Page: {page_name} ({page_id})")
        
        except MetaAPIError as e:
            logger.error(f"Meta API error discovering pages: {e}")
            raise
        
        return pages
    
    def _discover_ig_accounts(self, fb_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Discover Instagram Business Accounts linked to Facebook Pages.
        
        Args:
            fb_pages: List of discovered FB pages
            
        Returns:
            List of IG account data dicts
        """
        ig_accounts = []
        
        for page in fb_pages:
            page_id = page.get("page_id")
            if not page_id:
                continue
            
            try:
                # Get Instagram Business Account linked to this page
                response = self.client.get(
                    f"/{page_id}",
                    fields="instagram_business_account{id,username,name,profile_picture_url,followers_count,media_count}"
                )
                
                if not response:
                    continue
                
                ig_data = response.get("instagram_business_account")
                if not ig_data:
                    logger.debug(f"No IG account linked to FB Page {page_id}")
                    continue
                
                ig_id = ig_data.get("id")
                ig_username = ig_data.get("username", "")
                ig_name = ig_data.get("name", ig_username)
                
                if not ig_id:
                    continue
                
                # Upsert to ig_accounts table
                self.db.upsert_ig_account(
                    account_id=ig_id,
                    username=ig_username,
                    name=ig_name,
                    linked_fb_page_id=page_id
                )
                
                ig_accounts.append({
                    "account_id": ig_id,
                    "username": ig_username,
                    "name": ig_name,
                    "linked_fb_page_id": page_id,
                    "followers_count": ig_data.get("followers_count"),
                    "media_count": ig_data.get("media_count")
                })
                
                logger.info(f"Discovered IG Account: @{ig_username} ({ig_id}) linked to FB Page {page_id}")
            
            except MetaAPIError as e:
                logger.warning(f"Could not get IG account for page {page_id}: {e}")
                continue
        
        return ig_accounts
    
    def _create_customer_accounts(
        self, 
        fb_pages: List[Dict[str, Any]], 
        ig_accounts: List[Dict[str, Any]]
    ) -> int:
        """
        Create customer_accounts entries for discovered accounts.
        Accounts are created with customer_id=NULL (unassigned).
        
        Returns:
            Number of entries created
        """
        created = 0
        
        # Create entries for FB Pages
        for page in fb_pages:
            try:
                self.db.upsert_customer_account(
                    platform="facebook",
                    account_id=page["page_id"],
                    account_name=page.get("name")
                )
                created += 1
            except Exception as e:
                logger.warning(f"Failed to create customer_account for FB {page['page_id']}: {e}")
        
        # Create entries for IG Accounts
        for account in ig_accounts:
            try:
                self.db.upsert_customer_account(
                    platform="instagram",
                    account_id=account["account_id"],
                    account_name=account.get("username") or account.get("name")
                )
                created += 1
            except Exception as e:
                logger.warning(f"Failed to create customer_account for IG {account['account_id']}: {e}")
        
        logger.info(f"Created/updated {created} customer_accounts entries")
        return created


def run_account_discovery(config: Config) -> Dict[str, Any]:
    """
    Main entry point for account discovery.
    """
    db = Database(config)
    client = MetaClient(config)
    discovery = AccountDiscovery(config, db, client)
    
    return discovery.discover_all_accounts()
