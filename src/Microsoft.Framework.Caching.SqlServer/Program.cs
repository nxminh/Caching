// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Framework.Logging;

namespace Microsoft.Framework.Caching.SqlServer
{
    public class Program
    {
        private string _connectionString;
        private string _tableSchema;
        private string _tableName;

        private readonly ILogger _logger;

        public Program()
        {
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            _logger = loggerFactory.CreateLogger<Program>();
        }

        public void Main(string[] args)
        {
            //TODO1: use CommandLine and error checks
            _connectionString = args[0];
            _tableSchema = args[1] ?? "dbo";
            _tableName = args[2];

            CreateTableAndIndexes();
        }

        private void CreateTableAndIndexes()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                var sqlQueries = new SqlQueries(_tableSchema, _tableName);
                var command = new SqlCommand(sqlQueries.TableExists, connection);
                var reader = command.ExecuteReader(CommandBehavior.SingleRow);
                if (reader.Read())
                {
                    _logger.LogWarning(
                        $"Table with schema '{_tableSchema}' and name '{_tableName}' already exists. " +
                        "Provide a different table name and try again.");
                    return;
                }

                reader.Dispose();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        command = new SqlCommand(sqlQueries.CreateTable, connection, transaction);
                        command.ExecuteNonQuery();

                        command = new SqlCommand(
                            sqlQueries.CreateNonClusteredIndexOnExpirationTime,
                            connection,
                            transaction);
                        command.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("An error occurred while trying to create the table and index.", ex);
                        transaction.Rollback();
                    }
                }
            }
        }
    }
}