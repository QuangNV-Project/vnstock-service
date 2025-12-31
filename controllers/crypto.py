# controllers/crypto.py
"""
Crypto, Forex, and International market endpoints using MSN data source.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
import logging
import traceback

from vnstock.explorer.msn.quote import Quote as MSNQuote
from vnstock.explorer.msn.listing import Listing as MSNListing
from .common import safe_convert_to_records

router = APIRouter(tags=["MSN - Crypto/Forex/International"])
logger = logging.getLogger(__name__)


# =============================================================================
# MSN GENERAL ENDPOINTS
# =============================================================================

@router.get("/api/v1/msn/search")
async def msn_search_symbol(
    query: str = Query(..., description="Từ khóa tìm kiếm (VD: BTC, ETH, AAPL, EUR/USD)"),
    locale: Optional[str] = Query(None, description="Ngôn ngữ/thị trường: vi-vn, en-us, etc."),
    limit: int = Query(10, ge=1, le=50, description="Số lượng kết quả")
):
    """
    Tìm kiếm symbol trên MSN Money.

    Hỗ trợ:
    - **Crypto**: BTC, ETH, XRP, SOL, etc.
    - **Forex**: EUR/USD, GBP/USD, USD/JPY, etc.
    - **International stocks**: AAPL, GOOGL, MSFT, etc.
    - **Indices**: DJI, SPX, NDX, etc.
    """
    try:
        listing = MSNListing()
        df = listing.search_symbol_id(query=query, locale=locale, limit=limit)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error searching MSN symbol: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/msn/history")
async def msn_get_history(
    symbol_id: str = Query(..., description="MSN Symbol ID (lấy từ API search)"),
    start: str = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="Ngày kết thúc (YYYY-MM-DD)"),
    interval: str = Query("1D", description="Khung thời gian: 1D, 1W, 1M"),
    count_back: int = Query(365, ge=1, description="Số lượng dữ liệu trả về")
):
    """
    Lấy lịch sử giá từ MSN Money.

    **Quy trình:**
    1. Sử dụng `/api/v1/msn/search` để tìm symbol_id
    2. Dùng symbol_id để gọi API này

    **Example symbol_ids:**
    - Bitcoin: `a4kxhk` (tùy market)
    - Ethereum: Tìm qua search
    """
    try:
        if end is None:
            end = datetime.now().strftime("%Y-%m-%d")

        quote = MSNQuote(symbol_id=symbol_id)
        df = quote.history(start=start, end=end, interval=interval, count_back=count_back)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching MSN history for {symbol_id}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# CRYPTO ENDPOINTS
# =============================================================================

@router.get("/api/v1/crypto/search", tags=["Crypto"])
async def search_crypto(
    query: str = Query(..., description="Tên hoặc mã crypto (VD: BTC, ETH, Bitcoin)"),
    limit: int = Query(10, ge=1, le=50, description="Số lượng kết quả")
):
    """
    Tìm kiếm cryptocurrency.

    Shortcut cho MSN search với asset_type=crypto.
    """
    try:
        listing = MSNListing()
        df = listing.search_symbol_id(query=query, limit=limit)

        # Filter for crypto only if possible
        if 'asset_type' in df.columns:
            df = df[df['asset_type'].str.lower() == 'crypto']

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error searching crypto: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/crypto/history", tags=["Crypto"])
async def get_crypto_history(
    symbol_id: str = Query(..., description="MSN Symbol ID của crypto"),
    start: str = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="Ngày kết thúc (YYYY-MM-DD)"),
    interval: str = Query("1D", description="Khung thời gian: 1D, 1W, 1M"),
    count_back: int = Query(365, ge=1, description="Số lượng dữ liệu")
):
    """
    Lấy lịch sử giá cryptocurrency.

    **Hướng dẫn:**
    1. Gọi `/api/v1/crypto/search?query=BTC` để lấy symbol_id
    2. Sử dụng symbol_id để gọi API này
    """
    try:
        if end is None:
            end = datetime.now().strftime("%Y-%m-%d")

        quote = MSNQuote(symbol_id=symbol_id)
        df = quote.history(start=start, end=end, interval=interval,
                          count_back=count_back, asset_type="crypto")

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching crypto history for {symbol_id}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# FOREX ENDPOINTS
# =============================================================================

@router.get("/api/v1/forex/search", tags=["Forex"])
async def search_forex(
    query: str = Query(..., description="Cặp tiền tệ (VD: EUR/USD, GBP/USD)"),
    limit: int = Query(10, ge=1, le=50, description="Số lượng kết quả")
):
    """Tìm kiếm cặp tiền tệ forex."""
    try:
        listing = MSNListing()
        df = listing.search_symbol_id(query=query, limit=limit)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error searching forex: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/forex/history", tags=["Forex"])
async def get_forex_history(
    symbol_id: str = Query(..., description="MSN Symbol ID của forex pair"),
    start: str = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="Ngày kết thúc (YYYY-MM-DD)"),
    interval: str = Query("1D", description="Khung thời gian: 1D, 1W, 1M"),
    count_back: int = Query(365, ge=1, description="Số lượng dữ liệu")
):
    """Lấy lịch sử giá cặp tiền tệ."""
    try:
        if end is None:
            end = datetime.now().strftime("%Y-%m-%d")

        quote = MSNQuote(symbol_id=symbol_id)
        df = quote.history(start=start, end=end, interval=interval, count_back=count_back)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching forex history for {symbol_id}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

