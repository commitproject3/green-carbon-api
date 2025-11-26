"""Carbon emission calculations and scoring."""
from typing import Dict, List, Tuple
import numpy as np
from collections import defaultdict


# Emission factors (kgCO2e per 1 KRW)
EMISSION_FACTORS: Dict[str, float] = {
    "항공": 0.00060,
    "여행": 0.00060,
    "택시": 0.00018,
    "교통": 0.00012,
    "버스": 0.00012,
    "지하철": 0.00010,
    "카페": 0.00012,
    "음식점": 0.00012,
    "한식": 0.00012,
    "배달": 0.00015,
    "식품": 0.00008,
    "마트": 0.00008,
    "패션": 0.00020,
    "의류": 0.00020,
    "온라인": 0.00009,
    "문화": 0.00006,
    "병원": 0.00006,
    "기타": 0.00005,
}


def calculate_carbon_emission(total_amt: float, category_ratios: Dict[str, float]) -> float:
    """
    Calculate carbon emission: sum(total_amt * ratio_cat * EF[cat])
    """
    total_carbon = 0.0
    
    for category, ratio in category_ratios.items():
        ef = EMISSION_FACTORS.get(category, EMISSION_FACTORS["기타"])
        total_carbon += total_amt * ratio * ef
    
    return total_carbon


def calculate_carbon_score(carbon_kg: float, peer_carbons: List[float]) -> float:
    """
    Calculate carbon score as percentile rank (lower emission = higher score, 0-100).
    """
    if not peer_carbons:
        return 50.0  # Default score if no peer data
    
    # Convert to numpy array for percentile calculation
    peer_array = np.array(peer_carbons)
    
    # Calculate percentile rank (lower emission = higher score)
    # If carbon_kg is lower than X% of peers, score is (100 - X)
    percentile = (peer_array < carbon_kg).sum() / len(peer_array) * 100
    
    # Invert: lower emission should give higher score
    score = 100 - percentile
    
    # Ensure score is between 0 and 100
    return max(0.0, min(100.0, score))


def generate_recommendations(
    category_amounts: Dict[str, float],
    total_amt: float,
    category_ratios: Dict[str, float],
    top_n: int = 2
) -> List[Dict]:
    """
    Generate recommendations for top emitting categories.
    Assumes 15% reduction per category.
    """
    # Sort categories by emission (amount * ratio * EF)
    category_emissions = {}
    for category, ratio in category_ratios.items():
        ef = EMISSION_FACTORS.get(category, EMISSION_FACTORS["기타"])
        category_emissions[category] = total_amt * ratio * ef
    
    # Get top N emitting categories
    sorted_cats = sorted(category_emissions.items(), key=lambda x: x[1], reverse=True)
    top_cats = sorted_cats[:top_n]
    
    recommendations = []
    
    # Category-specific tips
    tips = {
        "카페": "텀블러 사용 + 배달 대신 매장 이용",
        "한식": "배달 1회→매장 전환",
        "패션": "필요한 것만 구매 + 중고 거래 고려",
        "식품": "로컬 생산 식품 선택 + 포장 줄이기",
        "온라인": "필요한 것만 구매 + 배송 횟수 줄이기",
        "택시": "대중교통 이용 + 자전거/도보 고려",
        "교통": "대중교통 이용 + 자전거/도보 고려",
        "항공": "필요한 경우만 이용 + 기차 대안 고려",
        "병원": "예방 건강 관리로 방문 횟수 줄이기",
        "문화": "온라인 콘텐츠 활용 + 지역 문화 시설 이용",
        "기타": "불필요한 소비 줄이기",
    }
    
    for category, emission in top_cats:
        # 15% reduction
        reduction_ratio = 0.15
        expected_reduction_kg = emission * reduction_ratio
        
        tip = tips.get(category, "해당 카테고리 소비 15% 줄이기")
        action = f"{category} 소비 15% 줄이기"
        
        recommendations.append({
            "category": category,
            "action": action,
            "expected_reduction_kg": round(expected_reduction_kg, 1),
            "tip": tip
        })
    
    return recommendations


