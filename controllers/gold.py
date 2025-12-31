# controllers/gold.py
"""
Gold price endpoints - SJC and BTMC gold prices.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging
import traceback

from vnstock.explorer.misc.gold_price import sjc_gold_price, btmc_goldprice
from .common import safe_convert_to_records

router = APIRouter(prefix="/api/v1/gold", tags=["Gold Price"])
logger = logging.getLogger(__name__)


@router.get("/sjc")
async def get_sjc_gold_price(
    date: Optional[str] = Query(None, description="Ngày tra cứu (YYYY-MM-DD). Mặc định là ngày hiện tại. Dữ liệu từ 2016-01-02")
):
    """
    Lấy giá vàng SJC.

    **Nguồn dữ liệu:** https://sjc.com.vn

    **Lưu ý:** Dữ liệu có sẵn từ ngày 2/1/2016.
    """
    try:
        df = sjc_gold_price(date=date)

        if df is None:
            return []

        return safe_convert_to_records(df)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching SJC gold price: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/btmc")
async def get_btmc_gold_price():
    """
    Lấy giá vàng Bảo Tín Minh Châu.

    **Nguồn dữ liệu:** http://btmc.vn

    Trả về giá vàng realtime từ nhiều loại vàng khác nhau.
    """
    try:
        df = btmc_goldprice()

        if df is None or df.empty:
            return []

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching BTMC gold price: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

