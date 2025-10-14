#!/usr/bin/env python3
# coding=utf-8
"""
Copyright Samuel Lloyd
2025
"""

import mysql.connector
from mysql.connector import errorcode


class Server:
    def __init__(self, connection_config):
        self.connection: mysql.connector.MySQLConnection = self._connect(
            connection_config
        )

        self.cursor = self.connection.cursor()

    def _connect(self, connection_config) -> mysql.connector.connection.MySQLConnection:
        try:
            cnx = mysql.connector.connect(**connection_config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            else:
                print(err)

        return cnx

    def __str__(self):
        return f"Server: {self.connection}"

    def databases(self, hidden=False):
        return Databases(self, server, hidden)


class Databases:
    def __init__(self, server, hidden=None):
        self.server = server
        server.cursor.execute("SHOW DATABASES")

        databases = [row[0] for row in server.cursor]

        self.databases = [Database(name, self) for name in databases]

        if not hidden:
            for i in ["information_schema", "mysql", "sys", "performance_schema"]:
                self.databases.remove(i)

    def __str__(self):
        return f"Databases: {[db.name for db in self.databases]}"

    def get(self, name):
        for db in self.databases:
            if db.name == name:
                return db
        raise ValueError(f"Database {name} not found")


class Database:
    def __init__(self, name, server):
        self.name = name
        self.server = server

    def __str__(self):
        return f"Database: {self.name}"

    def tables(self):
        self.server.cursor.execute(f"SHOW TABLES FROM {self.name}")

        self.server.cursor.execute(f"USE {self.name}")
        self.server.cursor.execute(
            "show full tables where Table_type like 'BASE TABLE';"
        )

        self.tables = [row[0] for row in self.server.cursor]
