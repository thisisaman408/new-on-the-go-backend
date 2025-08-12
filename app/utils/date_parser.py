"""
Robust date parsing for RSS feeds
Handles multiple date formats from different sources
"""

import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Union
from dateutil import parser as dateutil_parser
import pytz
import logging

logger = logging.getLogger(__name__)

class DateParser:
    """Robust date parser for RSS feed timestamps"""
    
    # Common RSS date formats (RFC 822, RFC 3339, ISO 8601, etc.)
    RSS_DATE_PATTERNS = [
        # RFC 822 format (most common in RSS)
        r'%a, %d %b %Y %H:%M:%S %Z',
        r'%a, %d %b %Y %H:%M:%S %z',
        r'%d %b %Y %H:%M:%S %Z',
        r'%d %b %Y %H:%M:%S %z',
        
        # ISO 8601 formats
        r'%Y-%m-%dT%H:%M:%S%z',
        r'%Y-%m-%dT%H:%M:%SZ',
        r'%Y-%m-%d %H:%M:%S',
        r'%Y-%m-%d %H:%M:%S%z',
        
        # Other common formats
        r'%Y-%m-%d',
        r'%d/%m/%Y %H:%M:%S',
        r'%m/%d/%Y %H:%M:%S',
        r'%d-%m-%Y %H:%M:%S',
    ]
    
    # Common timezone abbreviations mapping
    TIMEZONE_MAPPING = {
        'IST': 'Asia/Kolkata',
        'PST': 'US/Pacific',
        'EST': 'US/Eastern',
        'GMT': 'GMT',
        'UTC': 'UTC',
        'BST': 'Europe/London',
        'CET': 'Europe/Berlin',
        'JST': 'Asia/Tokyo',
        'CST': 'US/Central',
        'MST': 'US/Mountain'
    }
    
    @classmethod
    def parse_rss_date(cls, date_string: Union[str, None]) -> Optional[datetime]:
        """
        Parse RSS date string to UTC datetime
        
        Args:
            date_string: Date string from RSS feed
            
        Returns:
            datetime object in UTC or None if parsing fails
        """
        if not date_string or not isinstance(date_string, str):
            return None
            
        date_string = date_string.strip()
        if not date_string:
            return None
        
        # Try different parsing methods in order of preference
        parsed_date = (
            cls._try_dateutil_parser(date_string) or
            cls._try_pattern_matching(date_string) or
            cls._try_manual_parsing(date_string) or
            cls._try_fallback_parsing(date_string)
        )
        
        if parsed_date:
            # Ensure timezone awareness and convert to UTC
            return cls._normalize_to_utc(parsed_date)
        
        logger.warning(f"Failed to parse date: {date_string}")
        return None
    
    @classmethod
    def _try_dateutil_parser(cls, date_string: str) -> Optional[datetime]:
        """Try parsing with dateutil library (handles most formats)"""
        try:
            # Clean up common RSS date format issues
            cleaned = cls._clean_date_string(date_string)
            
            # Parse with dateutil
            parsed = dateutil_parser.parse(cleaned, fuzzy=True)
            return parsed
            
        except (ValueError, TypeError, AttributeError) as e:
            logger.debug(f"dateutil parsing failed for '{date_string}': {e}")
            return None
    
    @classmethod
    def _try_pattern_matching(cls, date_string: str) -> Optional[datetime]:
        """Try parsing with predefined patterns"""
        cleaned = cls._clean_date_string(date_string)
        
        for pattern in cls.RSS_DATE_PATTERNS:
            try:
                parsed = datetime.strptime(cleaned, pattern)
                return parsed
            except ValueError:
                continue
        
        return None
    
    @classmethod
    def _try_manual_parsing(cls, date_string: str) -> Optional[datetime]:
        """Manual parsing for specific RSS formats"""
        try:
            # Handle "Mon, 07 Aug 2023 15:30:00 +0530" format
            rfc822_pattern = r'(\w+),\s*(\d+)\s+(\w+)\s+(\d+)\s+(\d+):(\d+):(\d+)\s*([+-]\d{4}|\w+)?'
            match = re.match(rfc822_pattern, date_string)
            
            if match:
                day_name, day, month_name, year, hour, minute, second, tz = match.groups()
                
                # Convert month name to number
                month_map = {
                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                }
                month_num = month_map.get(month_name[:3], 1)
                
                # Create datetime
                dt = datetime(
                    int(year), month_num, int(day),
                    int(hour), int(minute), int(second)
                )
                
                # Handle timezone
                if tz:
                    if tz.startswith(('+', '-')):
                        # Offset format (+0530, -0800)
                        sign = 1 if tz[0] == '+' else -1
                        hours = int(tz[1:3])
                        minutes = int(tz[3:5])
                        offset_minutes = sign * (hours * 60 + minutes)
                        tz_obj = timezone(timedelta(minutes=offset_minutes))
                        dt = dt.replace(tzinfo=tz_obj)
                    else:
                        # Named timezone
                        tz_name = cls.TIMEZONE_MAPPING.get(tz, 'UTC')
                        tz_obj = pytz.timezone(tz_name)
                        dt = tz_obj.localize(dt)
                
                return dt
                
        except (ValueError, AttributeError) as e:
            logger.debug(f"Manual parsing failed for '{date_string}': {e}")
        
        return None
    
    @classmethod
    def _try_fallback_parsing(cls, date_string: str) -> Optional[datetime]:
        """Last resort parsing attempts"""
        try:
            # Try extracting just the date part
            date_only_pattern = r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})'
            match = re.search(date_only_pattern, date_string)
            
            if match:
                date_part = match.group(1)
                # Assume midnight UTC
                return datetime.strptime(date_part.replace('/', '-'), '%Y-%m-%d').replace(tzinfo=timezone.utc)
                
        except ValueError:
            pass
        
        return None
    
    @classmethod
    def _clean_date_string(cls, date_string: str) -> str:
        """Clean up date string for parsing"""
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', date_string.strip())
        
        # Fix common RSS date issues
        # Replace double timezone indicators
        cleaned = re.sub(r'\s+(GMT|UTC)\s*([+-]\d{4})', r' \2', cleaned)
        
        # Standardize timezone format
        cleaned = re.sub(r'\s*GMT\s*$', ' +0000', cleaned)
        cleaned = re.sub(r'\s*UTC\s*$', ' +0000', cleaned)
        
        # Fix IST timezone
        cleaned = re.sub(r'\s+IST\s*$', ' +0530', cleaned)
        
        return cleaned
    
    @classmethod
    def _normalize_to_utc(cls, dt: datetime) -> datetime:
        """Convert datetime to UTC timezone"""
        if dt.tzinfo is None:
            # Assume UTC for naive datetime
            return dt.replace(tzinfo=timezone.utc)
        
        # Convert to UTC
        return dt.astimezone(timezone.utc)
    
    @classmethod
    def format_for_database(cls, dt: Optional[datetime]) -> Optional[datetime]:
        """Format datetime for database storage (ensure UTC)"""
        if dt is None:
            return None
        
        return cls._normalize_to_utc(dt)
    
    @classmethod
    def get_current_utc(cls) -> datetime:
        """Get current UTC datetime"""
        return datetime.now(timezone.utc)

# Convenience functions
def parse_rss_date(date_string: Union[str, None]) -> Optional[datetime]:
    """Parse RSS date string to UTC datetime"""
    return DateParser.parse_rss_date(date_string)

def format_for_database(dt: Optional[datetime]) -> Optional[datetime]:
    """Format datetime for database storage"""
    return DateParser.format_for_database(dt)

# Test cases for validation
if __name__ == "__main__":
    test_dates = [
        "Mon, 07 Aug 2023 15:30:00 +0530",
        "Tue, 08 Aug 2023 10:15:00 GMT",
        "2023-08-07T15:30:00Z",
        "2023-08-07T15:30:00+05:30",
        "2023-08-07 15:30:00",
        "07 Aug 2023 15:30:00 IST",
        "August 7, 2023 3:30 PM",
        "07/08/2023 15:30:00",
        "Invalid date string"
    ]
    
    print("Testing RSS date parsing:")
    for test_date in test_dates:
        parsed = parse_rss_date(test_date)
        print(f"'{test_date}' -> {parsed}")
