#!/usr/bin/env python3
# coding=utf-8
"""
Copyright Samuel Lloyd
2025
"""

import argparse
import logging

from databaseversioncontrol import logger, log_format


def main():
    """
    Main function
    """
    pass


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--example", dest="example", type=str)
    args = parser.parse_args()

    # Log Run
    file_handler = logging.FileHandler("template_script.log")
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    print(args.example)

    # Run the main function and catch any exceptions
    raise SystemExit(main())
