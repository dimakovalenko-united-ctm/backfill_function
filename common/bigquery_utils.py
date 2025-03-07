#!/usr/bin/env python

from uuid import uuid4
from datetime import datetime
from .config import bq_client, DATASET, TABLE
from .models import MarketData, ResponseRecord

def prepare_insert_rows(data: list):
    rows_to_insert = [
        {
            "open": item.Open,
            "crypto_name": item.CryptoName,
            "crypto_symbol": item.CryptoSymbol,
            "ticker": item.Ticker,
            "fiat_currency": item.FiatCurrency,
            "source": item.Source,
            "close": item.Close,
            "high": item.High,
            "low": item.Low,
            "volume": item.Volume,
            "id": str(uuid4()),
            "timestamp": item.Timestamp,
            "dividends": item.Dividends if item.Dividends is not None else 0.0,
            "stock_splits": item.Stock_Splits if item.Stock_Splits is not None else 0.0,
            "is_deleted": False
        }
        for item in data
    ]
    success_records = [ResponseRecord(id=row["id"], timestamp=row["timestamp"]) for row in rows_to_insert]
    return rows_to_insert, success_records

def build_insert_query(rows_to_insert):
    unnest_array = ", ".join(
        f"STRUCT("
        f"'{row['id']}' AS id, "
        f"'{row['timestamp']}' AS timestamp, "
        f"{row['open']} AS open, "
        f"{row['close']} AS close, "
        f"{row['high']} AS high, "
        f"{row['low']} AS low, "
        f"{row['volume']} AS volume, "
        f"{row['dividends']} AS dividends, "
        f"{row['stock_splits']} AS stock_splits, "
        f"{str(row['is_deleted']).lower()} AS is_deleted, "
        f"'{row['crypto_name']}' AS crypto_name, "
        f"'{row['crypto_symbol']}' AS crypto_symbol, "
        f"'{row['ticker']}' AS ticker, "
        f"'{row['fiat_currency']}' AS fiat_currency, "
        f"'{row['source']}' AS source"
        f")"
        for row in rows_to_insert
    )
    query = f"""
        INSERT INTO `{DATASET}.{TABLE}` (
            id, open, close, high, low, volume, dividends, stock_splits, timestamp, is_deleted,
            crypto_name, crypto_symbol, ticker, fiat_currency, source
        )
        SELECT
            CAST(r.id AS STRING) AS id,
            CAST(r.open AS FLOAT64) AS open,
            CAST(r.close AS FLOAT64) AS close,
            CAST(r.high AS FLOAT64) AS high,
            CAST(r.low AS FLOAT64) AS low,
            CAST(r.volume AS FLOAT64) AS volume,
            CAST(r.dividends AS FLOAT64) AS dividends,
            CAST(r.stock_splits AS FLOAT64) AS stock_splits,
            CAST(r.timestamp AS TIMESTAMP) AS timestamp,
            r.is_deleted,
            r.crypto_name,
            r.crypto_symbol,
            r.ticker,
            r.fiat_currency,
            r.source
        FROM UNNEST([
            {unnest_array}
        ]) AS r;
    """
    return query

def execute_query(query):
    try:
        query_job = bq_client.query(query)
        query_job.result()  # Wait for the insert to complete
    except Exception as e:
        raise RuntimeError(f"BigQuery insert failed: {str(e)}")