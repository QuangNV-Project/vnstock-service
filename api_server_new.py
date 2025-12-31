# api_server.py
"""
FastAPI REST Service cho Vnstock
Triển khai REST API để tích hợp với Spring Microservices

Entry point chính - Import và đăng ký các routers từ controllers.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
import logging

# Import routers from controllers
from controllers import (
    health_router,
    quote_router,
    listing_router,
    company_router,
    finance_router,
    trading_router,
    screener_router,
    fund_router,
    crypto_router,
    market_router,
    gold_router,
    exchange_rate_router,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class ErrorResponse(BaseModel):
    error: str
    detail: str
    timestamp: str


# =============================================================================
# FASTAPI APP INITIALIZATION
# =============================================================================

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
    - **Gold**: Giá vàng SJC, BTMC
    - **Exchange Rate**: Tỷ giá ngoại tệ Vietcombank
    
    ## Data Sources
    - **VCI**: Vietcap Securities (Vietnam stocks)
    - **TCBS**: Techcombank Securities (Vietnam stocks)
    - **MSN**: MSN Money (Crypto, Forex, International stocks)
    - **Fmarket**: Fund data (Open-end funds in Vietnam)
    - **SJC/BTMC**: Gold prices
    - **Vietcombank**: Exchange rates
    
    ## Integration
    Designed for integration with Spring Boot Microservices ecosystem.
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


# =============================================================================
# MIDDLEWARE
# =============================================================================

# CORS Middleware - Allow Spring services to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# REGISTER ROUTERS
# =============================================================================

# Health endpoints (no prefix)
app.include_router(health_router)

# API v1 endpoints
app.include_router(quote_router)
app.include_router(listing_router)
app.include_router(company_router)
app.include_router(finance_router)
app.include_router(trading_router)
app.include_router(screener_router)
app.include_router(fund_router)
app.include_router(crypto_router)
app.include_router(market_router)
app.include_router(gold_router)
app.include_router(exchange_rate_router)


# =============================================================================
# ERROR HANDLERS
# =============================================================================

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
        host="0.0.0.0",
        port=9007,
        reload=True,  # Enable hot reload for development
        log_level="info"
    )

