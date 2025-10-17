#!/usr/bin/env python3
"""
Database Schema Backup Script

Exports all table and view CREATE statements from a MySQL server
and saves them as organized SQL files.
"""

from pathlib import Path
from datetime import datetime

from databaseversioncontrol.database import Server
from databaseversioncontrol.secrets_words import reporting_server_password


def backup_database_schemas(connection_config, output_path):
    """
    Backup all database schemas to a single SQL file.

    Parameters
    ----------
    connection_config : dict
        MySQL connection configuration
    output_path : str or Path
        Path where to save the schema file

    Returns
    -------
    tuple
        A tuple containing (table_definitions, views_definition) where:
        - table_definitions : dict
            Dictionary mapping database names to their table definitions
        - views_definition : dict
            Dictionary mapping database names to their view definitions
    """
    output_path = Path(output_path)

    # Collect all database schemas
    table_definitions = {}
    views_definition = {}

    with Server(connection_config) as server:
        print(f"Connected to: {server}")

        # Get all databases (excluding system databases)
        databases = server.databases()
        print(f"Found {len(databases)} databases")

        # Iterate through databases and collect all CREATE statements
        for db in databases:
            print(f"Processing Database: {db.name}")

            # Get all table CREATE statements
            all_table_creates = db.get_all_table_creates()
            if all_table_creates:
                table_definitions[db.name] = all_table_creates
                print(f"  Collected {len(all_table_creates)} table definitions")

            # Get all view CREATE statements
            all_view_creates = db.get_all_view_creates()
            if all_view_creates:
                views_definition[db.name] = all_view_creates
                print(f"  Collected {len(all_view_creates)} view definitions")

    create_schema_file(table_definitions, views_definition, output_path)

    return table_definitions, views_definition


def create_schema_file(table_definitions, views_definition, output_path):
    """
    Create a single SQL file with all database schemas.

    Parameters
    ----------
    table_definitions : dict
        Dictionary of database -> table definitions
    views_definition : dict
        Dictionary of database -> view definitions
    output_path : str or Path
        Path where to save the schema file
    """
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    script_content = []

    script_content.append("-- MySQL Database Complete Schema")
    script_content.append(f"-- Generated on: {datetime.now()}")
    script_content.append("-- This file contains all databases, tables, and views")
    script_content.append("")

    # Create databases and tables first
    for database in table_definitions:
        script_content.append("-- ========================================")
        script_content.append(f"-- Database: {database}")
        script_content.append("-- ========================================")
        script_content.append(f"CREATE DATABASE IF NOT EXISTS `{database}`;")
        script_content.append(f"USE `{database}`;")
        script_content.append("")

        # Add tables
        for table_name, table_definition in table_definitions[database].items():
            script_content.append(f"-- Table: {table_name}")
            script_content.append(table_definition + ";")
            script_content.append("")

    # Create views after all tables are created
    script_content.append("-- ========================================")
    script_content.append("-- VIEWS (Created after all tables)")
    script_content.append("-- ========================================")
    script_content.append("")

    for database in views_definition:
        script_content.append(f"-- Views for database: {database}")
        script_content.append(f"USE `{database}`;")
        script_content.append("")

        for view_name, view_definition in views_definition[database].items():
            script_content.append(f"-- View: {view_name}")
            script_content.append(view_definition + ";")
            script_content.append("")

    # Save the complete schema file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    schema_file_path = output_path / f"complete_database_schema_{timestamp}.sql"
    schema_file_path.write_text("\n".join(script_content), encoding="utf-8")

    print(f"Complete schema file created: {schema_file_path}")

    # Also create a "latest" version
    latest_file_path = output_path / "complete_database_schema_latest.sql"
    latest_file_path.write_text("\n".join(script_content), encoding="utf-8")
    print(f"Latest schema file updated: {latest_file_path}")


if __name__ == "__main__":
    # Configuration
    connection_config = {
        "user": "sam",
        "password": reporting_server_password,
        "host": "10.0.1.141",
        "port": "3306",
        "raise_on_warnings": True,
    }

    output_path = Path(".")

    try:
        # Backup all schemas to single file
        table_definitions, views_definition = backup_database_schemas(
            connection_config, output_path
        )

    except Exception as e:
        print(f"Error during backup: {e}")
        raise
