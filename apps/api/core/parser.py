"""CSV parser with robust date parsing."""
import csv
import io
from datetime import datetime
from typing import List, Dict, Optional
import re


def parse_date(date_str: str) -> Optional[str]:
    """
    Parse date string in various formats and return YYYY-MM-DD.
    Supports: YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, YYYYMMDD
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    date_str = date_str.strip()
    
    # Try YYYY-MM-DD
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    
    # Try YYYY/MM/DD
    try:
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    
    # Try YYYY.MM.DD
    try:
        dt = datetime.strptime(date_str, "%Y.%m.%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    
    # Try YYYYMMDD
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    
    return None


def parse_csv(file_content: bytes) -> List[Dict]:
    """
    Parse uploaded CSV file.
    Returns list of dictionaries with normalized column names.
    """
    # Decode file content
    try:
        content = file_content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            content = file_content.decode('cp949')  # Korean encoding
        except UnicodeDecodeError:
            content = file_content.decode('utf-8', errors='ignore')
    
    # Parse CSV
    reader = csv.DictReader(io.StringIO(content))
    rows = []
    
    for row in reader:
        # Normalize column names (case-insensitive, handle variations)
        normalized = {}
        for key, value in row.items():
            key_lower = key.lower().strip()
            # Map common variations
            if 'date' in key_lower:
                normalized['date'] = value
            elif 'amount' in key_lower or 'amt' in key_lower:
                normalized['amount'] = value
            elif 'merchant' in key_lower:
                normalized['merchant'] = value
            elif 'category' in key_lower or 'cat' in key_lower:
                normalized['category'] = value
            else:
                normalized[key] = value
        
        # Parse date
        if 'date' in normalized:
            parsed_date = parse_date(normalized['date'])
            if parsed_date:
                normalized['date'] = parsed_date
                rows.append(normalized)
    
    return rows


