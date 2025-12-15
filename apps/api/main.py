"""FastAPI application for carbon footprint prediction."""
from __future__ import annotations

from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Form
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict
import pandas as pd
from collections import defaultdict
from datetime import datetime
import os
from pathlib import Path
from bisect import bisect_right  # ★ 퍼센타일 계산용

# 내부 모듈
from core.parser import parse_csv, parse_text
from core.categorizer import (
    infer_category,
    get_top_categories,
    generate_cluster_name_hint,
)
from core.carbon import (
    calculate_carbon_emission,
    calculate_carbon_score,
    generate_recommendations,
)

# -------------------------------------------------------
# 경로 설정 (Render + Docker + Local 모두 호환)
# -------------------------------------------------------
BASE_DIR = Path(__file__).parent                 # /apps/api
DEFAULT_CSV = BASE_DIR / "data" / "segment_with_carbon.csv"
PEER_CSV = Path(os.getenv("PEER_CSV", str(DEFAULT_CSV)))

app = FastAPI(title="Carbon Footprint Prediction API", version="0.2.0")

# CORS (임시로 전체 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 피어 탄소분포 캐시
peer_carbons: List[float] = []
peer_sorted: List[float] = []  # ★ 퍼센타일 계산용 정렬본


def load_peer_distribution() -> None:
    """
    시작 시 피어 분포 CSV를 읽어 carbon_kg 분포를 메모리에 캐시합니다.
    - ENV PEER_CSV 우선
    - 상대경로면 BASE_DIR 기준으로 해석
    """
    global peer_carbons, peer_sorted

    csv_path = PEER_CSV
    if not csv_path.is_absolute():
        csv_path = BASE_DIR / csv_path

    if not csv_path.exists():
        raise FileNotFoundError(f"Peer distribution file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    peer_carbons[:] = (df["carbon_kg"].dropna().tolist() if "carbon_kg" in df.columns else [])
    peer_sorted[:] = sorted(peer_carbons)
    print(f"Loaded {len(peer_carbons)} peer carbon values from {csv_path}")


@app.on_event("startup")
async def startup_event():
    load_peer_distribution()


@app.get("/health")
async def health():
    return {"status": "ok"}


# -------------------------------------------------------
# 분류 유틸 (메인 유형/기후 유형/행태 유형)
# -------------------------------------------------------

# 키워드 패턴(소문자 비교) — infer_category의 결과 키나 원래 카테고리 텍스트를 느슨하게 커버
DELIVERY_PAT = ["배달", "배민", "요기요", "쿠팡이츠", "ubereats", "delivery", "음식배달", "외식"]
CAFE_PAT     = ["카페", "커피", "cafe", "coffee", "스타벅스", "이디야", "할리스", "투썸", "메가커피"]
TRANS_PAT    = ["교통", "대중교통", "지하철", "버스", "택시", "transport", "subway", "bus", "taxi", "tmoney", "카카오t"]

# 임계값(원하면 조정)
THRESH_DELIVERY_RATIO = 0.35
THRESH_CAFE_RATIO     = 0.25
THRESH_TRANS_RATIO    = 0.25

# 퍼센타일 컷 (피어 대비 80% 이상 = 고탄소, 20% 이하 = 저탄소)
PCTL_HIGH = 0.80
PCTL_LOW  = 0.20


def _contains_any(text: str, pats: List[str]) -> bool:
    t = text.lower()
    return any(p.lower() in t for p in pats)


def bucket_ratios(cat_ratio: Dict[str, float]) -> Dict[str, float]:
    """
    카테고리 비중 dict(cat_ratio)를 받아
    delivery/cafe/transport 비율을 대략적으로 추출.
    """
    d = c = tr = 0.0
    for k, v in cat_ratio.items():
        k_str = str(k)
        if _contains_any(k_str, DELIVERY_PAT):
            d += v
        elif _contains_any(k_str, CAFE_PAT):
            c += v
        elif _contains_any(k_str, TRANS_PAT):
            tr += v
    return {"delivery": d, "cafe": c, "transport": tr}


def classify_main_type(cat_ratio: Dict[str, float]) -> str:
    """
    메인 유형(카테고리형): 배달형/카페형/교통형/혼합형/기타형
    — 비율이 임계 이상이면 해당 형, 상위 두 비율이 비슷하면 혼합.
    """
    br = bucket_ratios(cat_ratio)

    if br["delivery"] >= THRESH_DELIVERY_RATIO:
        return "배달형"
    if br["cafe"] >= THRESH_CAFE_RATIO:
        return "카페형"
    if br["transport"] >= THRESH_TRANS_RATIO:
        return "교통형"

    # 상위 2개가 비슷(차이 ≤ 0.07)하고 둘 다 0.18 이상이면 혼합형
    top = sorted(br.items(), key=lambda x: x[1], reverse=True)
    if len(top) >= 2 and (top[0][1] - top[1][1]) <= 0.07 and (top[0][1] >= 0.18 and top[1][1] >= 0.18):
        return "혼합형"

    return "기타형"


def classify_climate_type(carbon_kg: float) -> str:
    """
    기후 유형(탄소형): 저탄소 / 보통 / 고탄소(개선 필요)
    — 피어 분포(peer_sorted)에서 단순 퍼센타일로 판정.
    """
    if not peer_sorted:
        return "보통"
    pos = bisect_right(peer_sorted, float(carbon_kg))
    pct = pos / len(peer_sorted)
    if pct >= PCTL_HIGH:
        return "고탄소(개선 필요)"
    if pct <= PCTL_LOW:
        return "저탄소"
    return "보통"


def classify_behavior_type(txn_count: int, avg_ticket: float) -> str:
    """
    행태 유형(옵션): 소액 다빈 / 고액 소빈 / 균형
    — 단순 규칙 기반
    """
    try:
        if txn_count >= 20 and avg_ticket <= 10000:
            return "소액 다빈"
        if txn_count <= 5 and avg_ticket >= 30000:
            return "고액 소빈"
    except Exception:
        pass
    return "균형"


# -------------------------------------------------------
# 파일/텍스트 둘 다 지원하는 /predict
# -------------------------------------------------------
@app.post("/predict")
async def predict(
    file: Optional[UploadFile] = File(None, description="CSV 파일 (선택)"),
    text: Optional[str] = Form(None, description="자유 텍스트 (선택)"),
    date: Optional[str] = Form(None, description="텍스트 행에 적용할 기본일자 YYYY-MM-DD (선택)"),
):
    """
    업로드된 CSV 또는 텍스트로부터 월별 탄소배출/점수/추천을 계산합니다.
    CSV 예: date,amount,merchant/category ...
    텍스트 예: "스타벅스 5000원, 배달의민족 15000원"
    """
    # Swagger가 file=""(빈 문자열)로 보내는 케이스 방어
    if isinstance(file, str) or (file and getattr(file, "filename", "") == ""):
        file = None

    # 1) CSV가 있으면 CSV 우선
    rows = None
    if file is not None:
        try:
            content = await file.read()
            rows = parse_csv(content)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"CSV parsing error: {str(e)}")

    # 2) 텍스트만 있는 경우
    if rows is None and text:
        try:
            rows = parse_text(text, date)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Text parsing error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=400, detail="Either CSV file or text input is required")

    # -----------------------------
    # 월별 집계
    # -----------------------------
    monthly = defaultdict(lambda: {
        "category_amounts": defaultdict(float),
        "total_amt": 0.0,
        "txn_count": 0,             # 거래건수
    })

    for row in rows:
        if "date" not in row or "amount" not in row:
            continue

        # 유연 날짜 파싱
        date_str = str(row["date"])
        dt = None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            continue

        year_month = dt.strftime("%Y-%m")
        try:
            amount = float(str(row["amount"]).replace(",", ""))
        except Exception:
            continue

        merchant = row.get("merchant") or row.get("merchant_name", "")
        category = row.get("category", "")
        cat = infer_category(merchant, category)

        monthly[year_month]["category_amounts"][cat] += amount
        monthly[year_month]["total_amt"] += amount
        monthly[year_month]["txn_count"] += 1

    # -----------------------------
    # 결과 생성
    # -----------------------------
    results = []
    for ym in sorted(monthly.keys()):
        total_amt = monthly[ym]["total_amt"]
        if total_amt <= 0:
            continue

        txn_count = monthly[ym]["txn_count"] or 1
        avg_ticket = total_amt / txn_count

        cat_amt = dict(monthly[ym]["category_amounts"])
        cat_ratio = {k: v / total_amt for k, v in cat_amt.items()}

        carbon_kg = calculate_carbon_emission(total_amt, cat_ratio)
        carbon_score = calculate_carbon_score(carbon_kg, peer_carbons)

        # 유형 분류
        main_type = classify_main_type(cat_ratio)
        climate_type = classify_climate_type(carbon_kg)
        behavior_type = classify_behavior_type(txn_count, avg_ticket)

        top3 = get_top_categories(cat_amt, top_n=3)
        cluster_name_hint = generate_cluster_name_hint(top3)

        recs = generate_recommendations(cat_amt, total_amt, cat_ratio, top_n=2)

        results.append({
            "month": ym,
            "total_amt": round(total_amt, 1),
            "cluster_name_hint": cluster_name_hint,      # 상위 카테고리 힌트 (기존)
            "carbon_kg": round(carbon_kg, 1),
            "carbon_score": round(carbon_score, 1),
            # 새 라벨 3종
            "main_type": main_type,
            "climate_type": climate_type,
            "behavior_type": behavior_type,
            "recommendations": recs,
        })

    return results


# -------------------------------------------------------
# JSON 바디로 보내고 싶은 사람들을 위한 /predict-text
# -------------------------------------------------------
class TextInput(BaseModel):
    text: str
    date: Optional[str] = None


@app.post("/predict-text")
async def predict_text(input_data: TextInput = Body(...)):
    try:
        rows = parse_text(input_data.text, input_data.date)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Text parsing error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=400, detail="No valid consumption rows in text")

    monthly = defaultdict(lambda: {
        "category_amounts": defaultdict(float),
        "total_amt": 0.0,
        "txn_count": 0,
    })

    for row in rows:
        if "date" not in row or "amount" not in row:
            continue

        date_str = str(row["date"])
        dt = None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            continue

        ym = dt.strftime("%Y-%m")
        try:
            amount = float(str(row["amount"]).replace(",", ""))
        except Exception:
            continue

        merchant = row.get("merchant") or row.get("merchant_name", "")
        category = row.get("category", "")
        cat = infer_category(merchant, category)

        monthly[ym]["category_amounts"][cat] += amount
        monthly[ym]["total_amt"] += amount
        monthly[ym]["txn_count"] += 1

    results = []
    for ym in sorted(monthly.keys()):
        total_amt = monthly[ym]["total_amt"]
        if total_amt <= 0:
            continue

        txn_count = monthly[ym]["txn_count"] or 1
        avg_ticket = total_amt / txn_count

        cat_amt = dict(monthly[ym]["category_amounts"])
        cat_ratio = {k: v / total_amt for k, v in cat_amt.items()}

        carbon_kg = calculate_carbon_emission(total_amt, cat_ratio)
        carbon_score = calculate_carbon_score(carbon_kg, peer_carbons)

        main_type = classify_main_type(cat_ratio)
        climate_type = classify_climate_type(carbon_kg)
        behavior_type = classify_behavior_type(txn_count, avg_ticket)

        top3 = get_top_categories(cat_amt, top_n=3)
        cluster_name_hint = generate_cluster_name_hint(top3)

        recs = generate_recommendations(cat_amt, total_amt, cat_ratio, top_n=2)

        results.append({
            "month": ym,
            "total_amt": round(total_amt, 1),
            "cluster_name_hint": cluster_name_hint,
            "carbon_kg": round(carbon_kg, 1),
            "carbon_score": round(carbon_score, 1),
            "main_type": main_type,
            "climate_type": climate_type,
            "behavior_type": behavior_type,
            "recommendations": recs,
        })

    return results
