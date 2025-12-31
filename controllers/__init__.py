# controllers/__init__.py
"""
Controllers package for Vnstock REST API.
Each controller handles a specific domain of endpoints.
"""

from .health import router as health_router
from .quote import router as quote_router
from .listing import router as listing_router
from .company import router as company_router
from .finance import router as finance_router
from .trading import router as trading_router
from .screener import router as screener_router
from .fund import router as fund_router
from .crypto import router as crypto_router
from .market import router as market_router
from .gold import router as gold_router
from .exchange_rate import router as exchange_rate_router

__all__ = [
    "health_router",
    "quote_router",
    "listing_router",
    "company_router",
    "finance_router",
    "trading_router",
    "screener_router",
    "fund_router",
    "crypto_router",
    "market_router",
    "gold_router",
    "exchange_rate_router",
]

