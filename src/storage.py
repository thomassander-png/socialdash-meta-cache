"""
Supabase Storage client for uploading and managing post images.
"""

import logging
import hashlib
import requests
from typing import Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class StorageClient:
    """Client for uploading images to Supabase Storage."""
    
    def __init__(self, supabase_url: str, service_role_key: str, bucket_name: str = "post-images"):
        """
        Initialize the storage client.
        
        Args:
            supabase_url: Supabase project URL (e.g., https://xxx.supabase.co)
            service_role_key: Supabase service role key for authenticated uploads
            bucket_name: Name of the storage bucket
        """
        self.supabase_url = supabase_url.rstrip('/')
        self.service_role_key = service_role_key
        self.bucket_name = bucket_name
        self.storage_url = f"{self.supabase_url}/storage/v1"
        
    def _get_headers(self) -> dict:
        """Get headers for authenticated requests."""
        return {
            "Authorization": f"Bearer {self.service_role_key}",
            "apikey": self.service_role_key
        }
    
    def _get_content_type(self, url: str) -> str:
        """Determine content type from URL or default to jpeg."""
        url_lower = url.lower()
        if '.png' in url_lower:
            return 'image/png'
        elif '.gif' in url_lower:
            return 'image/gif'
        elif '.webp' in url_lower:
            return 'image/webp'
        return 'image/jpeg'
    
    def _get_extension(self, content_type: str) -> str:
        """Get file extension from content type."""
        extensions = {
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'image/gif': '.gif',
            'image/webp': '.webp'
        }
        return extensions.get(content_type, '.jpg')
    
    def download_image(self, url: str) -> Optional[Tuple[bytes, str]]:
        """
        Download an image from a URL.
        
        Args:
            url: URL of the image to download
            
        Returns:
            Tuple of (image_bytes, content_type) or None if download fails
        """
        try:
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            content_type = response.headers.get('Content-Type', 'image/jpeg')
            if ';' in content_type:
                content_type = content_type.split(';')[0].strip()
            
            # Validate it's an image
            if not content_type.startswith('image/'):
                content_type = self._get_content_type(url)
            
            return response.content, content_type
            
        except Exception as e:
            logger.warning(f"Failed to download image from {url}: {e}")
            return None
    
    def upload_image(
        self, 
        image_data: bytes, 
        path: str, 
        content_type: str = "image/jpeg"
    ) -> Optional[str]:
        """
        Upload an image to Supabase Storage.
        
        Args:
            image_data: Raw image bytes
            path: Storage path (e.g., "facebook/page_id/post_id.jpg")
            content_type: MIME type of the image
            
        Returns:
            Public URL of the uploaded image, or None if upload fails
        """
        try:
            upload_url = f"{self.storage_url}/object/{self.bucket_name}/{path}"
            
            headers = self._get_headers()
            headers["Content-Type"] = content_type
            
            response = requests.post(
                upload_url,
                headers=headers,
                data=image_data,
                timeout=60
            )
            
            if response.status_code in [200, 201]:
                # Return public URL
                public_url = f"{self.supabase_url}/storage/v1/object/public/{self.bucket_name}/{path}"
                logger.info(f"Uploaded image to {path}")
                return public_url
            elif response.status_code == 409:
                # File already exists - return existing URL
                public_url = f"{self.supabase_url}/storage/v1/object/public/{self.bucket_name}/{path}"
                logger.debug(f"Image already exists at {path}")
                return public_url
            else:
                logger.error(f"Failed to upload image: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error uploading image to {path}: {e}")
            return None
    
    def check_exists(self, path: str) -> bool:
        """
        Check if a file already exists in storage.
        
        Args:
            path: Storage path to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            url = f"{self.supabase_url}/storage/v1/object/public/{self.bucket_name}/{path}"
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def cache_post_image(
        self,
        image_url: str,
        platform: str,
        account_id: str,
        post_id: str
    ) -> Optional[str]:
        """
        Download and cache a post image.
        
        Args:
            image_url: Original image URL from Meta API
            platform: "facebook" or "instagram"
            account_id: Page ID or Account ID
            post_id: Post ID or Media ID
            
        Returns:
            Permanent Supabase Storage URL, or None if caching fails
        """
        if not image_url:
            return None
        
        # Create a deterministic filename based on post_id
        # Clean post_id to be filesystem-safe
        safe_post_id = post_id.replace('_', '-').replace('/', '-')
        
        # Download the image first to determine extension
        result = self.download_image(image_url)
        if not result:
            return None
        
        image_data, content_type = result
        extension = self._get_extension(content_type)
        
        # Build storage path
        path = f"{platform}/{account_id}/{safe_post_id}{extension}"
        
        # Check if already cached
        if self.check_exists(path):
            public_url = f"{self.supabase_url}/storage/v1/object/public/{self.bucket_name}/{path}"
            logger.debug(f"Image already cached: {path}")
            return public_url
        
        # Upload to storage
        return self.upload_image(image_data, path, content_type)


def get_storage_client(config) -> Optional[StorageClient]:
    """
    Create a storage client from config.
    
    Returns None if storage is not configured.
    """
    if not config.supabase_storage_url or not config.supabase_service_role_key:
        logger.warning("Supabase Storage not configured - image caching disabled")
        return None
    
    return StorageClient(
        supabase_url=config.supabase_storage_url,
        service_role_key=config.supabase_service_role_key
    )
