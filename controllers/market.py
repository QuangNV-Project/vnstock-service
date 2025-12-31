# controllers/market.py
"""
Market endpoints - Indices, sectors, exchanges.
"""

from fastapi import APIRouter, HTTPException, Query
import logging
import traceback

from .common import safe_convert_to_records

router = APIRouter(prefix="/api/v1/market", tags=["Market"])
logger = logging.getLogger(__name__)


@router.get("/indices")
async def get_market_indices():
    """
    Lấy danh sách các chỉ số thị trường.

    Bao gồm: VN-Index, VN30, HNX-Index, UPCOM-Index, etc.
    """
    try:
        from vnstock.common.indices import get_all_indices
        df = get_all_indices()

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching market indices: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/index-groups")
async def get_index_groups():
    """Lấy danh sách nhóm chỉ số."""
    try:
        from vnstock.common.indices import get_all_index_groups
        groups = get_all_index_groups()

        return {"groups": groups}
    except Exception as e:
        logger.error(f"Error fetching index groups: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/indices-by-group")
async def get_indices_by_group(
    group: str = Query(..., description="Tên nhóm chỉ số")
):
    """Lấy các chỉ số trong một nhóm cụ thể."""
    try:
        from vnstock.common.indices import get_indices_by_group
        df = get_indices_by_group(group)

        if df is None:
            raise HTTPException(status_code=404, detail=f"Group '{group}' not found")

        return safe_convert_to_records(df)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching indices by group: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sectors")
async def get_all_sectors():
    """Lấy danh sách tất cả các ngành."""
    try:
        from vnstock.common.indices import get_all_sectors
        df = get_all_sectors()

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching sectors: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/exchanges")
async def get_all_exchanges():
    """Lấy danh sách các sàn giao dịch."""
    try:
        from vnstock.common.indices import get_all_exchanges
        exchanges = get_all_exchanges()

        return {"exchanges": exchanges}
    except Exception as e:
        logger.error(f"Error fetching exchanges: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

