#!/usr/bin/env python
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, validator

class MarketData(BaseModel):
    Open: float = Field(..., description="Opening price")
    CryptoName: str = Field(..., description="Name of the cryptocurrency")
    CryptoSymbol: str = Field(..., description="Symbol of the cryptocurrency")
    Ticker: str = Field(..., description="Ticker symbol used to identify the cryptocurrency (e.g., BTC-USD)")
    FiatCurrency: str = Field(..., description="Prices are in this fiat currency")    
    Source: str = Field(..., description="Location of the data source")
    Close: float = Field(..., description="Closing price")
    High: float = Field(..., description="Highest price")
    Low: float = Field(..., description="Lowest price")    
    Volume: float = Field(..., description="Trading volume")
    Dividends: Optional[float] = Field(None, description="Dividends")
    Stock_Splits: Optional[float] = Field(None, description="Stock splits")
    Timestamp: Optional[str] = Field(
        default_factory=lambda: datetime.now().isoformat(),
        description="Timestamp in ISO 8601 format"
    )

    @validator("Timestamp")
    def validate_timestamp(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        try:
            parsed_datetime = datetime.fromisoformat(value)
            if parsed_datetime.tzinfo is None:
                raise ValueError("Timestamp must be timezone-aware (include a timezone offset).")
        except ValueError as e:
            raise ValueError(f"Invalid ISO 8601 format for Timestamp: {e}")
        return value

class ResponseRecord(BaseModel):
    id: str
    timestamp: datetime

class SuccessResponse(BaseModel):
    status: str = "success"
    records: List[ResponseRecord]