"""FastAPI application for carbon footprint prediction."""
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import pandas as pd
from collections import defaultdict
from datetime import datetime
import os

from core.parser import parse_csv
from core.categorizer import infer_category, get_top_categories, generate_cluster_name_hint
from core.carbon import (
    calculate_carbon_emission,
    calculate_carbon_score,
    generate_recommendations
)

app = FastAPI(title="Carbon Footprint Prediction API")

# CORS middleware (temporarily allow all origins)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variable to cache peer distribution
peer_carbons: List[float] = []


def load_peer_distribution():
    """Load peer distribution from CSV at startup and cache carbon values."""
    global peer_carbons
    
    # CSV is at root level, go up two levels from apps/api/main.py
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "segment_with_carbon.csv")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Peer distribution file not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Extract carbon_kg values for percentile calculation
    if 'carbon_kg' in df.columns:
        peer_carbons = df['carbon_kg'].dropna().tolist()
    else:
        peer_carbons = []
    
    print(f"Loaded {len(peer_carbons)} peer carbon values")


@app.on_event("startup")
async def startup_event():
    """Load peer distribution at startup."""
    load_peer_distribution()


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/predict")
async def predict(file: UploadFile = File(...)):
    """
    Predict carbon footprint from uploaded CSV.
    
    Expected CSV columns (flexible):
    - date (required): YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, or YYYYMMDD
    - amount (required): transaction amount
    - merchant_name or merchant (optional): merchant name
    - category (optional): category name
    """
    # Read uploaded file
    try:
        file_content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")
    
    # Parse CSV
    try:
        rows = parse_csv(file_content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error parsing CSV: {str(e)}")
    
    if not rows:
        raise HTTPException(status_code=400, detail="No valid rows found in CSV")
    
    # Process transactions by month
    monthly_data = defaultdict(lambda: {
        'transactions': [],
        'category_amounts': defaultdict(float),
        'total_amt': 0.0
    })
    
    for row in rows:
        if 'date' not in row or 'amount' not in row:
            continue
        
        try:
            # Parse date to get year-month
            date_str = row['date']
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            year_month = dt.strftime("%Y-%m")
            
            # Parse amount
            amount = float(str(row['amount']).replace(',', ''))
            
            # Infer category
            merchant = row.get('merchant') or row.get('merchant_name', '')
            category = row.get('category', '')
            inferred_cat = infer_category(merchant, category)
            
            # Accumulate by month
            monthly_data[year_month]['transactions'].append({
                'date': date_str,
                'amount': amount,
                'category': inferred_cat
            })
            monthly_data[year_month]['category_amounts'][inferred_cat] += amount
            monthly_data[year_month]['total_amt'] += amount
            
        except (ValueError, KeyError) as e:
            # Skip invalid rows
            continue
    
    # Generate results per month
    results = []
    
    for year_month in sorted(monthly_data.keys()):
        month_data = monthly_data[year_month]
        total_amt = month_data['total_amt']
        category_amounts = dict(month_data['category_amounts'])
        
        if total_amt == 0:
            continue
        
        # Calculate category ratios
        category_ratios = {
            cat: amt / total_amt
            for cat, amt in category_amounts.items()
        }
        
        # Calculate carbon emission
        carbon_kg = calculate_carbon_emission(total_amt, category_ratios)
        
        # Calculate carbon score
        carbon_score = calculate_carbon_score(carbon_kg, peer_carbons)
        
        # Generate cluster name hint
        top_categories = get_top_categories(category_amounts, top_n=3)
        cluster_name_hint = generate_cluster_name_hint(top_categories)
        
        # Generate recommendations
        recommendations = generate_recommendations(
            category_amounts,
            total_amt,
            category_ratios,
            top_n=2
        )
        
        results.append({
            "month": year_month,
            "total_amt": round(total_amt, 1),
            "cluster_name_hint": cluster_name_hint,
            "carbon_kg": round(carbon_kg, 1),
            "carbon_score": round(carbon_score, 1),
            "recommendations": recommendations
        })
    
    return results

