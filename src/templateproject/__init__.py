#!/usr/bin/env python3
# coding=utf-8
"""
Copyright Samuel Lloyd
17/02/2025
"""

import importlib
import logging
import warnings

try:
    from template.untrackedpasswordfile import server_password
except (ModuleNotFoundError, ImportError):
    warnings.warn("Server Password not found")
    server_password = "Test"


# --- Set up logging ---


log_format = logging.Formatter(
    "%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a console handler

handler = logging.StreamHandler()
handler.setFormatter(log_format)
logger.addHandler(handler)

# Add a file handler

file_handler = logging.FileHandler("template.log")
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)


# --- import data for pip install ---


with importlib.resources.open_text("data", "template_data.csv") as file:
    data = file.read()


# --- Connect to MySQL Database ---


database_connection_config = {
    "user": "name",
    "host": "host",
    "passwd": "password",  # import from untracked file, dont type here!
    "port": 1234,
}

# def connect_to_mysql(config, attempts=3, delay=2):
#     attempt = 1
#     # Implement a reconnection routine
#     while attempt < attempts + 1:
#         try:
#             return mysql.connector.connect(**config)
#         except (mysql.connector.Error, IOError) as err:
#             if attempts is attempt:
#                 # Attempts to reconnect failed; returning None
#                 logger.error("Failed to connect, exiting without a connection: %s", err)
#                 raise err
#             logger.info(
#                 "Connection failed: %s. Retrying (%d/%d)...",
#                 err,
#                 attempt,
#                 attempts - 1,
#             )
#             # progressive reconnect delay
#             time.sleep(delay**attempt)
#             attempt += 1

