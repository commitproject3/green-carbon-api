"""Category inference & consumer-type classification."""
from __future__ import annotations
from typing import Optional, Dict, List, Tuple

# ---------------------------------------------------------
# 1) 키워드 규칙 (부분일치, 대소문자 무시)
#    - '배달' 관련 키워드 보강
# ---------------------------------------------------------
CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "카페": ["카페", "커피", "스타벅스", "starbucks"],
    "한식": ["한식", "국밥", "김밥", "백반", "순대", "고기", "찌개"],
    "패션": ["의류", "패션", "나이키", "nike", "아디다스", "adidas", "무신사", "자라", "zara", "유니클로", "uniqlo"],
    "식품": [
        "마트", "이마트", "emart", "홈플", "롯데마트", "costco", "코스트코",
        "편의점", "cu", "gs25", "세븐일레븐", "seven eleven", "세븐"
    ],
    "온라인": ["쿠팡", "coupang", "네이버페이", "naver pay", "스마일페이", "마켓컬리", "11번가", "g마켓", "gmarket", "ssg", "pay"],
    "택시": ["택시", "카카오t", "kakaot", "타다", "우버", "uber"],
    "교통": ["버스", "지하철", "전철", "철도", "ktx", "srt", "티머니", "tmoney", "교통"],
    "항공": ["항공", "대한항공", "korean air", "아시아나", "asiana", "제주항공", "진에어", "티웨이", "이스타"],
    "병원": ["병원", "의원", "치과", "한의원", "약국"],
    "문화": ["영화", "공연", "극장", "cgv", "메가박스", "megabox", "뮤지컬", "musical", "전시"],
    # 🟢 배달(배달앱) 강화
    "배달": [
        "배달", "배달의민족", "배민", "baemin", "요기요", "yogiyo",
        "쿠팡이츠", "coupang eats", "coupangeats", "ubereats", "요기패스", "배민페이"
    ],
}

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

# ---------------------------------------------------------
# 2) 카테고리 추론
# ---------------------------------------------------------
def infer_category(merchant: Optional[str] = None, category: Optional[str] = None) -> str:
    """
    가게명/카테고리 텍스트에서 키워드 부분일치로 소비 카테고리 추론.
    매칭 없으면 '기타' 반환.
    """
    text = f"{_norm(merchant)} {_norm(category)}"
    if not text.strip():
        return "기타"

    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if _norm(kw) in text:
                return cat

    # 온라인 흔한 토큰 보정
    if any(t in text for t in ["pay", "결제", "online", "on-line"]):
        return "온라인"

    return "기타"

# ---------------------------------------------------------
# 3) 상위 카테고리 & 클러스터 힌트 (기존 인터페이스 유지)
# ---------------------------------------------------------
def get_top_categories(category_amounts: Dict[str, float], top_n: int = 3) -> List[str]:
    """금액 기준 상위 N개 카테고리 이름만 반환 (기존 호환)."""
    sorted_cats = sorted(category_amounts.items(), key=lambda x: float(x[1]), reverse=True)
    return [cat for cat, _ in sorted_cats[:top_n]]

def generate_cluster_name_hint(top_categories: List[str]) -> str:
    """상위 카테고리로 '카페/한식형' 같은 힌트 생성."""
    if not top_categories:
        return "기타형"
    return "/".join(top_categories) + "형"

# ---------------------------------------------------------
# 4) 소비유형 라벨링 (새 기능)
#    - main_type: 지출비중 기반 (ex. '배달형', '카페/한식형', '혼합형')
#    - climate_type: 탄소 점수/강도 기반 (ex. '저탄소', '보통', '고탄소(개선 필요)')
#    - behavior_type: 건수/객단가 기반 (ex. '소액 다빈', '고액 소빈', '균형')
# ---------------------------------------------------------
def _top3_with_share(category_amounts: Dict[str, float], category_ratios: Dict[str, float]) -> List[Tuple[str, float]]:
    pairs = [(k, float(category_ratios.get(k, 0.0))) for k, v in category_amounts.items() if float(v) > 0]
    pairs.sort(key=lambda x: x[1], reverse=True)
    return pairs[:3]

def classify_types(
    category_amounts: Dict[str, float],
    category_ratios: Dict[str, float],
    carbon_score: Optional[float],
    carbon_kg: float,
    total_amt: float,
    txn_count: int
) -> Tuple[str, str, str]:
    """
    반환: (main_type, climate_type, behavior_type)
    """

    # ---- (1) 메인유형: 상위 카테고리 비중 ----
    top3 = _top3_with_share(category_amounts, category_ratios)
    if not top3:
        main_type = "혼합형"
    else:
        (c1, s1) = top3[0]
        (c2, s2) = top3[1] if len(top3) >= 2 else (None, 0.0)

        # 단일 지배형
        if s1 >= 0.45:
            main_type = f"{c1}형"
        # 복합형(상위 2개가 함께 큼)
        elif (s1 >= 0.30 and s2 >= 0.20) or (s1 >= 0.25 and s2 >= 0.25):
            main_type = f"{c1}/{c2}형" if c2 else f"{c1}형"
        else:
            main_type = "혼합형"

    # ---- (2) 기후유형: 점수 또는 강도 ----
    if carbon_score is not None and carbon_score >= 0:
        if carbon_score >= 70:
            climate_type = "저탄소"
        elif carbon_score >= 40:
            climate_type = "보통"
        else:
            climate_type = "고탄소(개선 필요)"
    else:
        # 점수 없으면 강도(kg/10만원) 기준
        intensity = (carbon_kg / max(total_amt, 1.0)) * 100_000.0
        if intensity < 8:
            climate_type = "저탄소"
        elif intensity < 14:
            climate_type = "보통"
        else:
            climate_type = "고탄소(개선 필요)"

    # ---- (3) 행태유형: 건수/객단가 ----
    avg_ticket = total_amt / max(txn_count, 1)
    if txn_count >= 15 and avg_ticket < 15_000:
        behavior_type = "소액 다빈"
    elif txn_count <= 5 and avg_ticket >= 50_000:
        behavior_type = "고액 소빈"
    else:
        behavior_type = "균형"

    return main_type, climate_type, behavior_type
