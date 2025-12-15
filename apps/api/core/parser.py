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


def parse_text(text: str, date: Optional[str] = None) -> List[Dict]:
    """
    Parse text input containing consumption records.
    
    Expected format examples:
    - "스타벅스 5000원, 배달의민족 15000원, 지하철 2000원"
    - "스타벅스 5000원\n배달의민족 15000원"
    - "스타벅스 5000, 배달의민족 15000" (원 생략 가능)
    
    Args:
        text: Text containing consumption records
        date: Optional date string (YYYY-MM-DD). If not provided, uses current date.
    
    Returns:
        List of dictionaries with 'date', 'amount', 'merchant' keys
    """
    from datetime import datetime
    
    # Use current date if not provided
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    else:
        parsed_date = parse_date(date)
        if parsed_date:
            date = parsed_date
        else:
            date = datetime.now().strftime("%Y-%m-%d")
    
    rows = []
    
    # Split by comma or newline
    items = re.split(r'[,，\n]+', text.strip())
    
    for item in items:
        item = item.strip()
        if not item:
            continue
        
        # Extract amount (numbers, possibly with commas, followed by optional "원")
        # Pattern: 숫자(쉼표 포함 가능) + 선택적 "원"
        amount_match = re.search(r'([\d,]+)\s*원?', item)
        if not amount_match:
            # Try without "원" - just numbers
            amount_match = re.search(r'([\d,]+)', item)
        
        if not amount_match:
            continue
        
        amount_str = amount_match.group(1).replace(',', '')
        try:
            amount = float(amount_str)
        except ValueError:
            continue
        
        # Extract merchant name (everything except the amount)
        merchant = item[:amount_match.start()].strip() + item[amount_match.end():].strip()
        merchant = re.sub(r'\s*원?\s*$', '', merchant).strip()
        
        # If merchant is empty, use the whole item (without amount)
        if not merchant:
            merchant = item.replace(amount_match.group(0), '').strip()
        
        if not merchant:
            merchant = "기타"
        
        rows.append({
            'date': date,
            'amount': str(amount),
            'merchant': merchant,
            'category': ''
        })
    
    return rows


