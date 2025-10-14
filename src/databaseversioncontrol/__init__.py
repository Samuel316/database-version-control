#!/usr/bin/env python3
# coding=utf-8
"""
Copyright Samuel Lloyd
2025
"""

# import importlib
import logging
# import warnings

# try:
#     from template.untrackedpasswordfile import server_password
# except (ModuleNotFoundError, ImportError):
#     warnings.warn("Server Password not found")
#     server_password = "Test"


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

# # --- Connect to MySQL Database ---

# database_connection_config = {
#     "user": "name",
#     "host": "host",
#     "passwd": "password",  # import from untracked file, dont type here!
#     "port": 1234,
# }
