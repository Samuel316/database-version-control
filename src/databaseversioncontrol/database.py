#!/usr/bin/env python3
# coding=utf-8
"""
Copyright Samuel Lloyd
2025
"""

import mysql.connector
from mysql.connector import errorcode

from sql_formatter.core import format_sql

from databaseversioncontrol.secrets_words import reporting_server_password


class Server:
    def __init__(self, connection_config):
        self.connection: mysql.connector.MySQLConnection = self._connect(
            connection_config
        )
        self.cursor = self.connection.cursor()

    def _connect(self, connection_config) -> mysql.connector.connection.MySQLConnection:
        try:
            cnx = mysql.connector.connect(**connection_config)
            return cnx
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise ConnectionError("Invalid username or password")
            else:
                raise ConnectionError(f"Database connection failed: {err}")

    def __str__(self):
        return f"Server: {self.connection.server_host}:{self.connection.server_port}"

    def databases(self, hidden=False):
        return Databases(self, hidden)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Databases:
    def __init__(self, server, hidden=None):
        self.server = server
        self.cursor = server.cursor
        self.cursor.execute("SHOW DATABASES")

        databases = [row[0] for row in self.cursor]

        if not hidden:
            system_dbs = ["information_schema", "mysql", "sys", "performance_schema"]
            databases = [db for db in databases if db not in system_dbs]

        self.databases = [Database(name, self.server) for name in databases]

    def __str__(self):
        return f"Databases: {[db.name for db in self.databases]}"

    def __getitem__(self, name):
        for db in self.databases:
            if db.name == name:
                return db
        raise KeyError(f"Database {name} not found")

    def __iter__(self):
        return iter(self.databases)

    def __len__(
        self,
    ):
        return len(self.databases)


class Tables:
    def __init__(self, server, database_name, table_type="BASE TABLE"):
        self.server = server
        self.cursor = server.cursor
        self.database_name = database_name
        self.table_type = table_type

    def get_all(self):
        self.cursor.execute(f"USE `{self.database_name}`")
        self.cursor.execute(f"SHOW FULL TABLES WHERE Table_type = '{self.table_type}'")
        return [row[0] for row in self.cursor]

    def __iter__(self):
        return iter(self.get_all())

    def get_create_statement(self, table_name):
        self.cursor.execute(f"USE `{self.database_name}`")
        if self.table_type == "VIEW":
            self.cursor.execute(f"SHOW CREATE VIEW `{table_name}`")
        else:
            self.cursor.execute(f"SHOW CREATE TABLE `{table_name}`")

        result = self.cursor.fetchone()
        if result:
            return format_sql(result[1])
        return None

    def get_all_create_statements(self):
        statements = {}
        for item in self.get_all():
            statements[item] = self.get_create_statement(item)
        return statements


class Database:
    def __init__(self, name, server):
        self.name = name
        self.server = server

    def __str__(self):
        return f"Database: {self.name}"

    def tables(self):
        return Tables(self.server, self.name, "BASE TABLE")

    def views(self):
        return Tables(self.server, self.name, "VIEW")

    def execute(self, query):
        self.server.cursor.execute(f"USE `{self.name}`")
        self.server.cursor.execute(query)
        return self.server.cursor.fetchall()

    def get_table_create_statement(self, table_name):
        tables = self.tables()
        return tables.get_create_statement(table_name)

    def get_view_create_statement(self, view_name):
        views = self.views()
        return views.get_create_statement(view_name)

    def get_all_table_creates(self):
        tables = self.tables()
        return tables.get_all_create_statements()

    def get_all_view_creates(self):
        views = self.views()
        return views.get_all_create_statements()


if __name__ == "__main__":
    # Example usage
    connection_config = {
        "user": "sam",
        "password": reporting_server_password,
        "host": "10.0.1.141",
        "port": "3306",
        "raise_on_warnings": True,
    }

    # Using context manager for automatic cleanup
    with Server(connection_config) as server:
        print(server)

        # Get all databases (excluding system databases)
        databases = server.databases()
        print(f"Found {len(databases)} databases")

        # Iterate through databases
        for db in databases:
            print(f"\nDatabase: {db.name}")

            # Get tables
            tables = db.tables()
            table_list = list(tables)
            print(f"  Tables ({len(table_list)}): {table_list}")

            # Get views
            views = db.views()
            view_list = list(views)
            print(f"  Views ({len(view_list)}): {view_list}")

            # Example: Get CREATE statements for first table and view
            if table_list:
                first_table = table_list[0]
                create_stmt = db.get_table_create_statement(first_table)
                print(f"  CREATE statement for table '{first_table}':")
                print(create_stmt)

            if view_list:
                first_view = view_list[0]
                create_stmt = db.get_view_create_statement(first_view)
                print(f"  CREATE statement for view '{first_view}':")
                print(create_stmt)

        # Example: Get all CREATE statements for a specific database
        if len(databases) > 0:
            example_db = databases.databases[0]
            print(f"\nGetting all CREATE statements for database: {example_db.name}")

            all_table_creates = example_db.get_all_table_creates()
            print(f"  Found {len(all_table_creates)} table CREATE statements")

            all_view_creates = example_db.get_all_view_creates()
            print(f"  Found {len(all_view_creates)} view CREATE statements")
