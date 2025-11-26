"""Category inference from merchant/category text using keyword rules."""
from typing import Optional, Dict, List


# Category keyword rules (case-insensitive substring match)
CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "카페": ["카페", "커피", "스타벅스"],
    "한식": ["한식", "국밥", "김밥", "백반", "순대", "고기", "찌개"],
    "패션": ["의류", "패션", "나이키", "아디다스", "무신사", "자라", "유니클로"],
    "식품": ["마트", "이마트", "홈플", "롯데마트", "코스트코", "편의점", "CU", "GS25", "세븐일레븐"],
    "온라인": ["쿠팡", "네이버페이", "스마일페이", "마켓컬리", "11번가", "G마켓", "SSG"],
    "택시": ["택시", "카카오T", "타다", "우버"],
    "교통": ["버스", "지하철", "전철", "철도", "KTX", "SRT", "티머니"],
    "항공": ["항공", "대한항공", "아시아나", "제주항공", "진에어", "티웨이"],
    "병원": ["병원", "의원", "치과", "한의원", "약국"],
    "문화": ["영화", "공연", "극장", "CGV", "메가박스", "뮤지컬"],
}


def infer_category(merchant: Optional[str] = None, category: Optional[str] = None) -> str:
    """
    Infer category from merchant or category text using keyword rules.
    Returns the matched category or "기타" if no match.
    """
    text = ""
    if merchant:
        text += " " + str(merchant)
    if category:
        text += " " + str(category)
    
    text = text.lower()
    
    # Check each category's keywords
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in text:
                return cat
    
    return "기타"


def get_top_categories(category_amounts: Dict[str, float], top_n: int = 3) -> List[str]:
    """Get top N categories by amount."""
    sorted_cats = sorted(category_amounts.items(), key=lambda x: x[1], reverse=True)
    return [cat for cat, _ in sorted_cats[:top_n]]


def generate_cluster_name_hint(top_categories: List[str]) -> str:
    """Generate cluster name hint from top categories."""
    if not top_categories:
        return "기타형"
    return "/".join(top_categories) + "형"


