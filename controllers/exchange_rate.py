# controllers/exchange_rate.py
"""
Exchange rate endpoints - VCB exchange rates.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
import logging
import traceback

from vnstock.explorer.misc.exchange_rate import vcb_exchange_rate
from .common import safe_convert_to_records

router = APIRouter(prefix="/api/v1/exchange-rate", tags=["Exchange Rate"])
logger = logging.getLogger(__name__)


@router.get("/vcb")
async def get_vcb_exchange_rate(
    date: Optional[str] = Query(None, description="Ngày tra cứu (YYYY-MM-DD). Mặc định là ngày hiện tại")
):
    """
    Lấy tỷ giá ngoại tệ Vietcombank.

    **Nguồn dữ liệu:** https://www.vietcombank.com.vn

    **Thông tin trả về:**
    - currency_code: Mã tiền tệ (USD, EUR, JPY, etc.)
    - currency_name: Tên tiền tệ
    - buy_cash: Giá mua tiền mặt
    - buy_transfer: Giá mua chuyển khoản
    - sell: Giá bán
    """
    try:
        # Nếu không truyền date, dùng ngày hiện tại
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        df = vcb_exchange_rate(date=date)

        if df is None or df.empty:
            return []

        return safe_convert_to_records(df)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching VCB exchange rate: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

