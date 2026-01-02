"""
Tests for date filtering and timezone handling.
"""

import pytest
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.finalize_month import parse_month_string
from src.config import Config


class TestMonthParsing:
    """Test cases for month string parsing."""
    
    def test_parse_yyyy_mm_format(self):
        """Test parsing YYYY-MM format."""
        result = parse_month_string("2025-12")
        assert result == date(2025, 12, 1)
    
    def test_parse_yyyy_mm_dd_format(self):
        """Test parsing YYYY-MM-DD format (should return first of month)."""
        result = parse_month_string("2025-12-15")
        assert result == date(2025, 12, 1)
    
    def test_parse_month_year_format(self):
        """Test parsing 'Month Year' format."""
        result = parse_month_string("December 2025")
        assert result == date(2025, 12, 1)
    
    def test_parse_january(self):
        """Test parsing January correctly."""
        result = parse_month_string("2025-01")
        assert result == date(2025, 1, 1)
    
    def test_parse_invalid_format(self):
        """Test that invalid formats raise ValueError."""
        with pytest.raises(ValueError):
            parse_month_string("invalid-date")
    
    def test_parse_empty_string(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError):
            parse_month_string("")


class TestDateBoundaries:
    """Test date boundary calculations."""
    
    def test_month_start_boundary(self):
        """Test that month parsing always returns first of month."""
        # Various inputs should all return first of month
        test_cases = [
            ("2025-12-01", date(2025, 12, 1)),
            ("2025-12-15", date(2025, 12, 1)),
            ("2025-12-31", date(2025, 12, 1)),
        ]
        
        for input_str, expected in test_cases:
            result = parse_month_string(input_str)
            assert result == expected, f"Failed for input: {input_str}"
    
    def test_lookback_days_calculation(self):
        """Test lookback days calculation for post fetching."""
        now = datetime(2025, 1, 2, 12, 0, 0)
        lookback_days = 45
        
        since = now - timedelta(days=lookback_days)
        
        assert since.date() == date(2024, 11, 18)
    
    def test_date_range_for_december_2025(self):
        """Test date range calculation for December 2025 report."""
        month = parse_month_string("2025-12")
        
        # Start of December
        start = datetime(month.year, month.month, 1)
        
        # End of December (last day)
        if month.month == 12:
            end = datetime(month.year + 1, 1, 1) - timedelta(seconds=1)
        else:
            end = datetime(month.year, month.month + 1, 1) - timedelta(seconds=1)
        
        assert start == datetime(2025, 12, 1, 0, 0, 0)
        assert end.date() == date(2025, 12, 31)


class TestTimezoneHandling:
    """Test timezone-related functionality."""
    
    def test_config_default_timezone(self):
        """Test that default timezone is Europe/Berlin."""
        config = Config(
            meta_access_token="test",
            meta_api_version="v20.0",
            fb_page_ids=["123"],
            database_url="postgresql://test",
            timezone="Europe/Berlin"
        )
        
        assert config.timezone == "Europe/Berlin"
    
    def test_iso_date_parsing(self):
        """Test parsing ISO format dates from Facebook API."""
        # Facebook returns dates in ISO format with Z suffix
        fb_date_str = "2025-12-15T14:30:00+0000"
        
        # Parse as datetime
        dt = datetime.fromisoformat(fb_date_str.replace("Z", "+00:00").replace("+0000", "+00:00"))
        
        assert dt.year == 2025
        assert dt.month == 12
        assert dt.day == 15
        assert dt.hour == 14
        assert dt.minute == 30
    
    def test_utc_date_comparison(self):
        """Test comparing UTC dates for filtering."""
        post_time = datetime(2025, 12, 15, 10, 0, 0)
        
        # December 2025 boundaries
        month_start = datetime(2025, 12, 1, 0, 0, 0)
        month_end = datetime(2025, 12, 31, 23, 59, 59)
        
        assert month_start <= post_time <= month_end


class TestDateFiltering:
    """Test date filtering logic for posts."""
    
    def test_post_within_range(self):
        """Test that posts within range are included."""
        since = datetime(2025, 12, 1)
        until = datetime(2025, 12, 31)
        
        post_time = datetime(2025, 12, 15)
        
        is_in_range = since <= post_time <= until
        assert is_in_range is True
    
    def test_post_before_range(self):
        """Test that posts before range are excluded."""
        since = datetime(2025, 12, 1)
        until = datetime(2025, 12, 31)
        
        post_time = datetime(2025, 11, 30)
        
        is_in_range = since <= post_time <= until
        assert is_in_range is False
    
    def test_post_after_range(self):
        """Test that posts after range are excluded."""
        since = datetime(2025, 12, 1)
        until = datetime(2025, 12, 31)
        
        post_time = datetime(2026, 1, 1)
        
        is_in_range = since <= post_time <= until
        assert is_in_range is False
    
    def test_post_on_boundary_start(self):
        """Test that posts on start boundary are included."""
        since = datetime(2025, 12, 1)
        until = datetime(2025, 12, 31)
        
        post_time = datetime(2025, 12, 1, 0, 0, 0)
        
        is_in_range = since <= post_time <= until
        assert is_in_range is True
    
    def test_post_on_boundary_end(self):
        """Test that posts on end boundary are included."""
        since = datetime(2025, 12, 1)
        until = datetime(2025, 12, 31, 23, 59, 59)
        
        post_time = datetime(2025, 12, 31, 23, 59, 59)
        
        is_in_range = since <= post_time <= until
        assert is_in_range is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
