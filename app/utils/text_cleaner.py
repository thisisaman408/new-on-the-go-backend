"""
Comprehensive text cleaning for RSS content
Removes HTML, normalizes text, handles encoding issues
"""

import re
import html
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup, NavigableString
import unicodedata
import logging

logger = logging.getLogger(__name__)

class TextCleaner:
    """Comprehensive text cleaning for RSS content"""
    
    # HTML tags that should be completely removed (including content)
    REMOVE_TAGS = [
        'script', 'style', 'meta', 'link', 'noscript', 
        'iframe', 'embed', 'object', 'applet', 'form'
    ]
    
    # HTML tags to preserve as text formatting
    PRESERVE_TAGS = {
        'p': '\n\n',
        'br': '\n',
        'div': '\n',
        'h1': '\n\n',
        'h2': '\n\n', 
        'h3': '\n\n',
        'h4': '\n\n',
        'h5': '\n\n',
        'h6': '\n\n',
        'li': '\n• ',
        'blockquote': '\n"',
        'hr': '\n---\n'
    }
    
    # Common RSS feed junk patterns
    JUNK_PATTERNS = [
        # Social media sharing text
        r'share\s+on\s+(facebook|twitter|linkedin|whatsapp)',
        r'follow\s+us\s+on\s+(facebook|twitter|instagram)',
        r'like\s+us\s+on\s+facebook',
        
        # Advertisement indicators
        r'advertisement\s*:?\s*',
        r'\[?\s*ad\s*\]?',
        r'sponsored\s+content',
        
        # Newsletter/subscription prompts
        r'subscribe\s+to\s+our\s+newsletter',
        r'sign\s+up\s+for\s+updates',
        
        # Copyright and legal text
        r'©\s*\d{4}.*?all\s+rights\s+reserved',
        r'terms\s+of\s+use',
        r'privacy\s+policy',
        
        # Common RSS metadata
        r'filed\s+under\s*:',
        r'tags\s*:',
        r'category\s*:',
        
        # Read more links
        r'read\s+more\s*\.{3}',
        r'continue\s+reading',
        r'full\s+story\s+here',
        
        # Image captions patterns
        r'image\s*:\s*getty\s+images',
        r'photo\s*:\s*reuters',
        r'source\s*:\s*[a-zA-Z\s]+',
    ]
    
    @classmethod
    def clean_html_content(cls, html_content: Optional[str]) -> Optional[str]:
        """
        Clean HTML content from RSS feeds
        
        Args:
            html_content: Raw HTML content from RSS
            
        Returns:
            Cleaned plain text or None
        """
        if not html_content or not isinstance(html_content, str):
            return None
        
        try:
            # Decode HTML entities first
            decoded = html.unescape(html_content)
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(decoded, 'html.parser')
            
            # Remove unwanted tags completely
            for tag_name in cls.REMOVE_TAGS:
                for tag in soup.find_all(tag_name):
                    tag.decompose()
            
            # Convert HTML to text with formatting preservation
            text = cls._extract_text_with_formatting(soup)
            
            # Clean up the text
            cleaned = cls._post_process_text(text)
            
            return cleaned if cleaned and cleaned.strip() else None
            
        except Exception as e:
            logger.warning(f"HTML cleaning failed: {e}")
            # Fallback to simple HTML tag removal
            return cls._simple_html_strip(html_content)
    
    @classmethod
    def _extract_text_with_formatting(cls, soup: BeautifulSoup) -> str:
        """Extract text while preserving some formatting"""
        def extract_text_recursive(element):
            if isinstance(element, NavigableString):
                return str(element)
            
            tag_name = element.name.lower() if element.name else ''
            text_parts = []
            
            # Add opening formatting for certain tags
            if tag_name in cls.PRESERVE_TAGS:
                text_parts.append(cls.PRESERVE_TAGS[tag_name])
            
            # Process children
            for child in element.children:
                child_text = extract_text_recursive(child)
                if child_text:
                    text_parts.append(child_text)
            
            # Add closing formatting for certain tags
            if tag_name == 'blockquote':
                text_parts.append('"')
            
            return ''.join(text_parts)
        
        return extract_text_recursive(soup)
    
    @classmethod
    def _post_process_text(cls, text: str) -> str:
        """Post-process extracted text"""
        if not text:
            return ""
        
        # Normalize unicode characters
        text = unicodedata.normalize('NFKC', text)
        
        # Remove RSS junk patterns
        for pattern in cls.JUNK_PATTERNS:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)
        
        # Clean up whitespace
        text = cls._normalize_whitespace(text)
        
        # Remove empty lines and excessive spacing
        lines = [line.strip() for line in text.split('\n')]
        cleaned_lines = []
        
        for line in lines:
            if line and line not in cleaned_lines[-3:]:  # Avoid immediate duplicates
                cleaned_lines.append(line)
        
        # Join lines with proper spacing
        result = '\n'.join(cleaned_lines)
        
        # Final cleanup
        result = re.sub(r'\n{3,}', '\n\n', result)  # Max 2 consecutive newlines
        result = result.strip()
        
        return result
    
    @classmethod
    def _normalize_whitespace(cls, text: str) -> str:
        """Normalize whitespace in text"""
        # Replace multiple spaces with single space
        text = re.sub(r' {2,}', ' ', text)
        
        # Replace tabs with spaces
        text = text.replace('\t', ' ')
        
        # Normalize line endings
        text = re.sub(r'\r\n?', '\n', text)
        
        return text
    
    @classmethod
    def _simple_html_strip(cls, html_content: str) -> str:
        """Simple HTML tag removal fallback"""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', html_content)
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Clean whitespace
        text = cls._normalize_whitespace(text)
        
        return text.strip()
    
    @classmethod
    def extract_summary(cls, content: str, max_length: int = 300) -> str:
        """
        Extract a clean summary from content
        
        Args:
            content: Cleaned text content
            max_length: Maximum summary length
            
        Returns:
            Summary text
        """
        if not content:
            return ""
        
        # Get first paragraph or sentence cluster
        paragraphs = content.split('\n\n')
        first_paragraph = paragraphs[0].strip()
        
        if len(first_paragraph) <= max_length:
            return first_paragraph
        
        # Try to cut at sentence boundary
        sentences = first_paragraph.split('. ')
        summary = ""
        
        for sentence in sentences:
            potential_summary = summary + sentence + '. '
            if len(potential_summary) > max_length:
                break
            summary = potential_summary
        
        # If we have something, return it
        if summary.strip():
            return summary.strip()
        
        # Otherwise, cut at word boundary
        words = first_paragraph.split()
        summary = ""
        
        for word in words:
            potential_summary = summary + word + ' '
            if len(potential_summary) > max_length - 3:
                break
            summary = potential_summary
        
        return (summary.strip() + '...') if summary.strip() else first_paragraph[:max_length-3] + '...'
    
    @classmethod
    def calculate_reading_time(cls, content: str, words_per_minute: int = 200) -> int:
        """
        Calculate estimated reading time in minutes
        
        Args:
            content: Text content
            words_per_minute: Average reading speed
            
        Returns:
            Reading time in minutes (minimum 1)
        """
        if not content:
            return 1
        
        # Count words
        word_count = len(content.split())
        
        # Calculate reading time
        reading_time = max(1, round(word_count / words_per_minute))
        
        return reading_time
    
    @classmethod
    def clean_rss_item(cls, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean an entire RSS item's text content
        
        Args:
            item_data: Dictionary with RSS item data
            
        Returns:
            Dictionary with cleaned content
        """
        cleaned_item = item_data.copy()
        
        # Clean title
        if 'title' in cleaned_item:
            title = cls.clean_html_content(cleaned_item['title'])
            cleaned_item['title'] = title.replace('\n', ' ').strip() if title else None
        
        # Clean content/description
        content_fields = ['content', 'description', 'summary']
        for field in content_fields:
            if field in cleaned_item and cleaned_item[field]:
                cleaned_content = cls.clean_html_content(cleaned_item[field])
                cleaned_item[field] = cleaned_content
                
                # Calculate additional metadata
                if cleaned_content:
                    cleaned_item['word_count'] = len(cleaned_content.split())
                    cleaned_item['reading_time_minutes'] = cls.calculate_reading_time(cleaned_content)
                    
                    # Generate summary if content is long
                    if field == 'content' and len(cleaned_content) > 300:
                        cleaned_item['summary'] = cls.extract_summary(cleaned_content)
        
        return cleaned_item

# Convenience functions
def clean_html_content(html_content: Optional[str]) -> Optional[str]:
    """Clean HTML content from RSS feeds"""
    return TextCleaner.clean_html_content(html_content)

def extract_summary(content: str, max_length: int = 300) -> str:
    """Extract summary from content"""
    return TextCleaner.extract_summary(content, max_length)

def calculate_reading_time(content: str, words_per_minute: int = 200) -> int:
    """Calculate reading time in minutes"""
    return TextCleaner.calculate_reading_time(content, words_per_minute)

# Test the cleaner
if __name__ == "__main__":
    test_html = """
    <div class="article">
        <h1>Test Article Title</h1>
        <p>This is a <strong>test article</strong> with various HTML elements.</p>
        <script>alert('This should be removed');</script>
        <p>Second paragraph with <a href="http://example.com">a link</a>.</p>
        <ul>
            <li>List item 1</li>
            <li>List item 2</li>
        </ul>
        <div>Advertisement: Buy our product!</div>
        <p>Final paragraph.</p>
    </div>
    """
    
    print("Testing HTML cleaning:")
    cleaned = clean_html_content(test_html)
    print(f"Original: {test_html}")
    print(f"Cleaned: {cleaned}")
    print(f"Reading time: {calculate_reading_time(cleaned or '')} minutes")
