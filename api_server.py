# api_server.py
"""
FastAPI REST Service cho Vnstock
Triển khai REST API để tích hợp với Spring Microservices

Features:
- Stock Quote: Lịch sử giá, dữ liệu intraday, price depth
- Company: Thông tin công ty, cổ đông, ban lãnh đạo
- Finance: Báo cáo tài chính
- Trading: Bảng giá, thống kê giao dịch
- Listing: Danh sách mã chứng khoán
- Screener: Lọc cổ phiếu
- Fund: Quỹ mở
- Crypto/Forex: Dữ liệu từ MSN
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, List, Any, Dict
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import numpy as np
import logging
import traceback

# Import vnstock modules
from vnstock import Quote, Listing, Company, Finance, Trading, Screener, Fund
from vnstock.explorer.msn.quote import Quote as MSNQuote
from vnstock.explorer.msn.listing import Listing as MSNListing
from vnstock.explorer.misc.gold_price import sjc_gold_price, btmc_goldprice
from vnstock.explorer.misc.exchange_rate import vcb_exchange_rate

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def safe_convert_to_records(df: Any, default_value: Any = None) -> List[Dict]:
    """
    Safely convert DataFrame/Series to list of records.
    Handles various data types and edge cases including NaN values.
    """
    if default_value is None:
        default_value = []

    if df is None:
        return default_value

    # Handle Series
    if isinstance(df, pd.Series):
        result = []
        for idx, val in df.items():
            # Handle NaN in Series
            if pd.isna(val):
                val = None
            elif isinstance(val, (np.floating, np.integer)):
                val = None if np.isnan(val) else val.item()
            result.append({"index": str(idx), "value": val})
        return result

    # Handle DataFrame
    if isinstance(df, pd.DataFrame):
        if df.empty:
            return default_value

        # Handle MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = ['_'.join(map(str, col)).strip('_') for col in df.columns.values]

        # Convert to records
        records = df.to_dict(orient="records")

        # Clean up special types (datetime, numpy types, NaN)
        for record in records:
            for key, value in record.items():
                if value is None:
                    continue
                # Handle NaN/Inf float values
                if isinstance(value, float):
                    if np.isnan(value) or np.isinf(value):
                        record[key] = None
                # Handle numpy types
                elif isinstance(value, (np.floating)):
                    if np.isnan(value) or np.isinf(value):
                        record[key] = None
                    else:
                        record[key] = float(value)
                elif isinstance(value, np.integer):
                    record[key] = int(value)
                # Handle datetime
                elif isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.isoformat() if pd.notna(value) else None
                # Handle pandas NA
                elif pd.isna(value):
                    record[key] = None

        return records

    # Handle dict
    if isinstance(df, dict):
        return [df]

    # Handle list
    if isinstance(df, list):
        return df

    return default_value

# Pydantic Models for API responses
class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str

class ErrorResponse(BaseModel):
    error: str
    detail: str
    timestamp: str

# Initialize FastAPI
app = FastAPI(
    title="Vnstock REST API",
    description="""
    REST API Service cho dữ liệu chứng khoán Việt Nam và thị trường quốc tế.
    
    ## Features
    - **Quote**: Lịch sử giá cổ phiếu (OHLCV), dữ liệu intraday, price depth
    - **Listing**: Danh sách mã chứng khoán, phân ngành ICB
    - **Company**: Thông tin công ty, cổ đông, ban lãnh đạo, tin tức
    - **Finance**: Báo cáo tài chính (Balance Sheet, Income Statement, Cash Flow)
    - **Trading**: Bảng giá, thống kê giao dịch, foreign trade, insider deals
    - **Screener**: Lọc cổ phiếu theo điều kiện
    - **Fund**: Dữ liệu quỹ mở (Fmarket)
    - **Crypto/Forex**: Dữ liệu crypto và forex từ MSN
    
    ## Data Sources
    - **VCI**: Vietcap Securities (Vietnam stocks)
    - **TCBS**: Techcombank Securities (Vietnam stocks)
    - **MSN**: MSN Money (Crypto, Forex, International stocks)
    - **Fmarket**: Fund data (Open-end funds in Vietnam)
    
    ## Integration
    Designed for integration with Spring Boot Microservices ecosystem.
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Middleware - Allow Spring services to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# HEALTH CHECK ENDPOINTS
# =============================================================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint cho service discovery."""
    return HealthResponse(
        status="healthy",
        service="vnstock-api",
        timestamp=datetime.now().isoformat()
    )


@app.get("/ready", tags=["Health"])
async def readiness_check():
    """Readiness probe cho Kubernetes."""
    try:
        # Try a simple operation to verify service is ready
        listing = Listing(source="vci")
        _ = listing.all_symbols()  # Actually call to verify connection
        return {"ready": True, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"ready": False, "error": str(e)}
        )


# =============================================================================
# QUOTE ENDPOINTS
# =============================================================================

@app.get("/api/v1/quote/history", tags=["Quote"])
async def get_quote_history(
    symbol: str = Query(..., description="Mã cổ phiếu (VD: ACB, FPT, VNM)"),
    start: str = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="Ngày kết thúc (YYYY-MM-DD)"),
    interval: str = Query("1D", description="Khung thời gian: 1m, 5m, 15m, 30m, 1H, 1D, 1W, 1M"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci, tcbs")
):
    """
    Lấy lịch sử giá cổ phiếu (OHLCV).

    **Intervals:**
    - `1m`, `5m`, `15m`, `30m`: Intraday data
    - `1H`: Hourly data
    - `1D`: Daily data (default)
    - `1W`: Weekly data
    - `1M`: Monthly data
    """
    try:
        quote = Quote(source=source, symbol=symbol.upper())
        df = quote.history(start=start, end=end, interval=interval)

        if df is None or df.empty:
            return []

        # Convert DataFrame to list of dicts
        records = df.to_dict(orient="records")

        # Convert timestamp/datetime to string
        for record in records:
            for key, value in record.items():
                if isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.isoformat()

        return records

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching history for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/quote/intraday", tags=["Quote"])
async def get_intraday_data(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    page_size: int = Query(100, ge=1, le=1000, description="Số lượng records"),
    page: int = Query(1, ge=1, description="Số trang"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy dữ liệu giao dịch trong ngày (tick data)."""
    try:
        quote = Quote(source=source, symbol=symbol.upper())
        df = quote.intraday(page_size=page_size, page=page)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching intraday for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/quote/price-depth", tags=["Quote"])
async def get_price_depth(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy dữ liệu độ sâu thị trường (bid/ask)."""
    try:
        quote = Quote(source=source, symbol=symbol.upper())
        df = quote.price_depth()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching price depth for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# LISTING ENDPOINTS
# =============================================================================

@app.get("/api/v1/listing/symbols", tags=["Listing"])
async def get_all_symbols(
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách tất cả mã cổ phiếu."""
    try:
        listing = Listing(source=source)
        df = listing.all_symbols()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/listing/symbols-by-group", tags=["Listing"])
async def get_symbols_by_group(
    group: str = Query("VN30", description="Nhóm: VN30, HNX30, VN100, VNALL, etc."),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách mã theo nhóm (VN30, HNX30, etc.)."""
    try:
        listing = Listing(source=source)
        df = listing.symbols_by_group(group=group)

        if df is None or df.empty:
            return []


        return [{"index": str(idx), "name": val} for idx, val in df.items()]

    except Exception as e:
        logger.error(f"Error fetching symbols by group {group}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/listing/symbols-by-exchange", tags=["Listing"])
async def get_symbols_by_exchange(
    lang: str = Query("vi", description="Ngôn ngữ: vi, en"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách mã theo sàn (HOSE, HNX, UPCOM)."""
    try:
        listing = Listing(source=source)
        df = listing.symbols_by_exchange(lang=lang)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching symbols by exchange: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/listing/industries", tags=["Listing"])
async def get_industries(
    lang: str = Query("vi", description="Ngôn ngữ: vi, en"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách phân ngành ICB."""
    try:
        listing = Listing(source=source)
        df = listing.symbols_by_industries(lang=lang)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching industries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/listing/industries-icb", tags=["Listing"])
async def get_icb_industries(
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy bảng mã ngành ICB."""
    try:
        listing = Listing(source=source)
        df = listing.industries_icb()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching ICB industries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/listing/futures", tags=["Listing"])
async def get_futures(
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách hợp đồng tương lai."""
    try:
        listing = Listing(source=source)
        df = listing.all_future_indices()

        if df is None or df.empty:
            return []

        return [{"index": str(idx), "recordName": val} for idx, val in df.items()]

    except Exception as e:
        logger.error(f"Error fetching futures: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/listing/covered-warrants", tags=["Listing"])
async def get_covered_warrants(
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách chứng quyền có bảo đảm."""
    try:
        listing = Listing(source=source)
        df = listing.all_covered_warrant()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching covered warrants: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# COMPANY ENDPOINTS
# =============================================================================

@app.get("/api/v1/company/overview", tags=["Company"])
async def get_company_overview(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci, tcbs")
):
    """Lấy thông tin tổng quan công ty."""
    try:
        company = Company(source=source, symbol=symbol.upper())
        result = company.overview()

        if result is None:
            return {}

        if isinstance(result, pd.DataFrame):
            return result.to_dict(orient="records")
        return result

    except Exception as e:
        logger.error(f"Error fetching company overview for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/shareholders", tags=["Company"])
async def get_shareholders(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách cổ đông lớn."""
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.shareholders()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching shareholders for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/officers", tags=["Company"])
async def get_officers(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    filter_by: str = Query("all", description="Lọc: all, working, resigned"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách ban lãnh đạo."""
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.officers(filter_by=filter_by)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching officers for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/subsidiaries", tags=["Company"])
async def get_subsidiaries(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách công ty con."""
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.subsidiaries()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching subsidiaries for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/news", tags=["Company"])
async def get_company_news(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy tin tức công ty."""
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.news()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching news for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/events", tags=["Company"])
async def get_company_events(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy sự kiện công ty (ĐHCĐ, chia cổ tức, etc.)."""
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.events()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching events for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# FINANCE ENDPOINTS
# =============================================================================

@app.get("/api/v1/finance/balance-sheet", tags=["Finance"])
async def get_balance_sheet(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    period: str = Query("quarter", description="Kỳ: quarter, annual"),
    lang: str = Query("vi", description="Ngôn ngữ: vi, en"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci, tcbs")
):
    """Lấy bảng cân đối kế toán."""
    try:
        finance = Finance(source=source, symbol=symbol.upper(), period=period)
        df = finance.balance_sheet(lang=lang)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching balance sheet for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/finance/income-statement", tags=["Finance"])
async def get_income_statement(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    period: str = Query("quarter", description="Kỳ: quarter, annual"),
    lang: str = Query("vi", description="Ngôn ngữ: vi, en"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci, tcbs")
):
    """Lấy báo cáo kết quả kinh doanh."""
    try:
        finance = Finance(source=source, symbol=symbol.upper(), period=period)
        df = finance.income_statement(lang=lang)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching income statement for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/finance/cash-flow", tags=["Finance"])
async def get_cash_flow(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    period: str = Query("quarter", description="Kỳ: quarter, annual"),
    lang: str = Query("vi", description="Ngôn ngữ: vi, en"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci, tcbs")
):
    """Lấy báo cáo lưu chuyển tiền tệ."""
    try:
        finance = Finance(source=source, symbol=symbol.upper(), period=period)
        df = finance.cash_flow(lang=lang)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching cash flow for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/finance/ratio", tags=["Finance"])
async def get_financial_ratios(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    period: str = Query("quarter", description="Kỳ: quarter, annual"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci, tcbs")
):
    """Lấy các chỉ số tài chính."""
    try:
        finance = Finance(source=source, symbol=symbol.upper(), period=period)
        df = finance.ratio()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching ratios for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# TRADING ENDPOINTS
# =============================================================================

@app.get("/api/v1/trading/price-board", tags=["Trading"])
async def get_price_board(
    symbols: str = Query(..., description="Danh sách mã cổ phiếu, phân cách bằng dấu phẩy (VD: VPB,ACB,FPT)"),
    source: str = Query("tcbs", description="Nguồn dữ liệu: vci, tcbs")
):
    """
    Lấy bảng giá cho nhiều mã cổ phiếu.

    **Lưu ý:** Cần truyền danh sách mã, không phải một mã đơn lẻ.
    """
    try:
        symbols_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]

        if not symbols_list:
            raise HTTPException(status_code=400, detail="Cần ít nhất một mã cổ phiếu")

        trading = Trading(source=source, symbol=symbols_list[0])

        if source.lower() == "vci":
            df = trading.price_board(symbols_list=symbols_list, flatten_columns=True)
        else:  # tcbs
            df = trading.price_board(symbol_ls=symbols_list)

        return safe_convert_to_records(df)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching price board for {symbols}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SCREENER ENDPOINTS
# =============================================================================

@app.get("/api/v1/screener/stocks", tags=["Screener"])
async def screen_stocks(
    exchange: str = Query("HOSE,HNX,UPCOM", description="Sàn: HOSE, HNX, UPCOM (phân cách bằng dấu phẩy)"),
    limit: int = Query(50, ge=1, le=200, description="Số lượng kết quả"),
    lang: str = Query("vi", description="Ngôn ngữ: vi, en")
):
    """
    Lọc cổ phiếu theo điều kiện.

    Sử dụng TCBS Stock Screener để lọc cổ phiếu.
    """
    try:
        screener = Screener(source="tcbs")
        params = {"exchangeName": exchange}
        df = screener.stock(params=params, limit=limit, lang=lang)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error screening stocks: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/screener/stocks/filter", tags=["Screener"])
async def screen_stocks_with_filters(
    params: dict = None,
    limit: int = Query(50, ge=1, le=200, description="Số lượng kết quả"),
    lang: str = Query("vi", description="Ngôn ngữ: vi, en")
):
    """
    Lọc cổ phiếu nâng cao với nhiều điều kiện.

    **Body params example:**
    ```json
    {
        "exchangeName": "HOSE,HNX",
        "marketCap": [10000000000000, 50000000000000]
    }
    ```
    """
    try:
        screener = Screener(source="tcbs")
        if params is None:
            params = {"exchangeName": "HOSE,HNX,UPCOM"}
        df = screener.stock(params=params, limit=limit, lang=lang)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error screening stocks with filters: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# FUND ENDPOINTS (Fmarket)
# =============================================================================

@app.get("/api/v1/fund/listing", tags=["Fund"])
async def get_fund_listing(
    fund_type: str = Query("", description="Loại quỹ: STOCK, BOND, BALANCED, hoặc rỗng để lấy tất cả")
):
    """
    Lấy danh sách các quỹ mở từ Fmarket.

    **Loại quỹ:**
    - `STOCK`: Quỹ cổ phiếu
    - `BOND`: Quỹ trái phiếu
    - `BALANCED`: Quỹ cân bằng
    - Rỗng: Tất cả các quỹ
    """
    try:
        fund = Fund()
        df = fund.listing(fund_type=fund_type)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching fund listing: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/fund/filter", tags=["Fund"])
async def filter_fund(
    symbol: str = Query("", description="Tên viết tắt của quỹ (short_name)")
):
    """Tìm kiếm quỹ theo tên viết tắt."""
    try:
        fund = Fund()
        df = fund.filter(symbol=symbol)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error filtering fund: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/fund/top-holding", tags=["Fund"])
async def get_fund_top_holding(
    symbol: str = Query(..., description="Tên viết tắt của quỹ (VD: SSISCA, VCBF-BCF)")
):
    """Lấy danh sách top holdings của quỹ."""
    try:
        fund = Fund()
        df = fund.details.top_holding(symbol=symbol)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching fund top holding for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/fund/industry-holding", tags=["Fund"])
async def get_fund_industry_holding(
    symbol: str = Query(..., description="Tên viết tắt của quỹ")
):
    """Lấy phân bổ theo ngành của quỹ."""
    try:
        fund = Fund()
        df = fund.details.industry_holding(symbol=symbol)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching fund industry holding for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/fund/nav-report", tags=["Fund"])
async def get_fund_nav_report(
    symbol: str = Query(..., description="Tên viết tắt của quỹ")
):
    """Lấy lịch sử NAV (Giá trị tài sản ròng) của quỹ."""
    try:
        fund = Fund()
        df = fund.details.nav_report(symbol=symbol)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching fund NAV report for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/fund/asset-holding", tags=["Fund"])
async def get_fund_asset_holding(
    symbol: str = Query(..., description="Tên viết tắt của quỹ")
):
    """Lấy phân bổ tài sản của quỹ."""
    try:
        fund = Fund()
        df = fund.details.asset_holding(symbol=symbol)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching fund asset holding for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# CRYPTO & INTERNATIONAL MARKET ENDPOINTS (MSN)
# =============================================================================

@app.get("/api/v1/msn/search", tags=["MSN - Crypto/Forex/International"])
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


@app.get("/api/v1/msn/history", tags=["MSN - Crypto/Forex/International"])
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


@app.get("/api/v1/crypto/search", tags=["Crypto"])
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


@app.get("/api/v1/crypto/history", tags=["Crypto"])
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


@app.get("/api/v1/forex/search", tags=["Forex"])
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


@app.get("/api/v1/forex/history", tags=["Forex"])
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


# =============================================================================
# ADDITIONAL TRADING ENDPOINTS
# =============================================================================

@app.get("/api/v1/trading/foreign-trade", tags=["Trading"])
async def get_foreign_trade(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    start: str = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="Ngày kết thúc (YYYY-MM-DD)"),
    source: str = Query("tcbs", description="Nguồn dữ liệu: tcbs")
):
    """Lấy dữ liệu giao dịch khối ngoại."""
    try:
        trading = Trading(source=source, symbol=symbol.upper())

        if end is None:
            end = datetime.now().strftime("%Y-%m-%d")

        df = trading.foreign_trade(start=start, end=end)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching foreign trade for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/trading/insider-deals", tags=["Trading"])
async def get_insider_deals(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("tcbs", description="Nguồn dữ liệu: tcbs")
):
    """Lấy dữ liệu giao dịch nội bộ (insider deals)."""
    try:
        trading = Trading(source=source, symbol=symbol.upper())
        df = trading.insider_deal()

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching insider deals for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/trading/prop-trade", tags=["Trading"])
async def get_prop_trade(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("tcbs", description="Nguồn dữ liệu: tcbs")
):
    """Lấy dữ liệu giao dịch tự doanh."""
    try:
        trading = Trading(source=source, symbol=symbol.upper())
        df = trading.prop_trade()

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching prop trade for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# INDEX & MARKET DATA ENDPOINTS
# =============================================================================

@app.get("/api/v1/market/indices", tags=["Market"])
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


@app.get("/api/v1/market/index-groups", tags=["Market"])
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


@app.get("/api/v1/market/indices-by-group", tags=["Market"])
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


@app.get("/api/v1/market/sectors", tags=["Market"])
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


@app.get("/api/v1/market/exchanges", tags=["Market"])
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


# =============================================================================
# GOLD PRICE ENDPOINTS
# =============================================================================

@app.get("/api/v1/gold/sjc", tags=["Gold Price"])
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


@app.get("/api/v1/gold/btmc", tags=["Gold Price"])
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


# =============================================================================
# EXCHANGE RATE ENDPOINTS
# =============================================================================

@app.get("/api/v1/exchange-rate/vcb", tags=["Exchange Rate"])
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


# =============================================================================
# ADDITIONAL COMPANY ENDPOINTS
# =============================================================================

@app.get("/api/v1/company/profile", tags=["Company"])
async def get_company_profile(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("tcbs", description="Nguồn dữ liệu: tcbs")
):
    """
    Lấy thông tin mô tả chi tiết công ty.

    Bao gồm: lịch sử hình thành, ngành nghề, mô tả hoạt động kinh doanh.
    """
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.profile()

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching company profile for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/dividends", tags=["Company"])
async def get_company_dividends(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    page_size: int = Query(15, ge=1, le=100, description="Số lượng kết quả"),
    page: int = Query(0, ge=0, description="Số trang (bắt đầu từ 0)"),
    source: str = Query("tcbs", description="Nguồn dữ liệu: tcbs")
):
    """
    Lấy lịch sử trả cổ tức của công ty.

    **Thông tin trả về:**
    - Ngày thực hiện (exercise_date)
    - Loại cổ tức (tiền mặt/cổ phiếu)
    - Tỷ lệ/Giá trị cổ tức
    """
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.dividends(page_size=page_size, page=page)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching dividends for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/affiliate", tags=["Company"])
async def get_company_affiliate(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci")
):
    """
    Lấy danh sách công ty liên kết.

    Các công ty mà công ty hiện tại có sở hữu cổ phần.
    """
    try:
        company = Company(source=source, symbol=symbol.upper())
        df = company.affiliate()

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching affiliate for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ADDITIONAL LISTING ENDPOINTS
# =============================================================================

@app.get("/api/v1/listing/government-bonds", tags=["Listing"])
async def get_government_bonds(
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách trái phiếu chính phủ."""
    try:
        listing = Listing(source=source)
        df = listing.all_government_bonds()

        if df is None:
            return []

        return safe_convert_to_records(df)

    except Exception as e:
        logger.error(f"Error fetching government bonds: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/listing/bonds", tags=["Listing"])
async def get_all_bonds(
    source: str = Query("vci", description="Nguồn dữ liệu")
):
    """Lấy danh sách tất cả trái phiếu."""
    try:
        listing = Listing(source=source)
        df = listing.all_bonds()

        if df is None:
            return []

        return safe_convert_to_records(df)

    except Exception as e:
        logger.error(f"Error fetching bonds: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ADDITIONAL TRADING ENDPOINTS
# =============================================================================

@app.get("/api/v1/trading/stats", tags=["Trading"])
async def get_trading_stats(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    start: str = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="Ngày kết thúc (YYYY-MM-DD)"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci, tcbs")
):
    """
    Lấy thống kê giao dịch của cổ phiếu.

    Bao gồm: khối lượng, giá trị giao dịch theo ngày.
    """
    try:
        if end is None:
            end = datetime.now().strftime("%Y-%m-%d")

        trading = Trading(source=source, symbol=symbol.upper())
        df = trading.trading_stats(start=start, end=end)

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching trading stats for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/trading/order-stats", tags=["Trading"])
async def get_order_stats(
    symbol: str = Query(..., description="Mã cổ phiếu"),
    source: str = Query("vci", description="Nguồn dữ liệu: vci")
):
    """
    Lấy thống kê lệnh đặt của cổ phiếu.

    Phân tích khối lượng lệnh mua/bán theo các mức giá.
    """
    try:
        trading = Trading(source=source, symbol=symbol.upper())
        df = trading.order_stats()

        return safe_convert_to_records(df)
    except Exception as e:
        logger.error(f"Error fetching order stats for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=f"HTTP {exc.status_code}",
            detail=exc.detail,
            timestamp=datetime.now().isoformat()
        ).model_dump()
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.exception("Unhandled exception")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal Server Error",
            detail=str(exc),
            timestamp=datetime.now().isoformat()
        ).model_dump()
    )


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api_server:app",
        host="localhost",
        port=9007,
        reload=True,  # Enable hot reload for development
        log_level="info"
    )

