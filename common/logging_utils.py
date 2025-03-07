#!/usr/bin/env python
import os
import logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging import Client

def setup_logger():
    log_name = os.getenv("GOOGLE_CLOUD_LOG_NAME", "service_historical_crypto_prices")
    cloud_logging_handler = CloudLoggingHandler(Client(), name=log_name)
    logging.basicConfig(level=logging.INFO, handlers=[cloud_logging_handler])
    return logging.getLogger(__name__)