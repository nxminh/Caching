// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;

namespace Microsoft.Framework.Caching.SqlServer
{
    public class Program
    {
        private string _connectionString;
        private string _tableSchema;
        private string _tableName;
        private SqlQueries _sqlQueries;

        public void Main(string[] args)
        {
            //TODO1: use CommandLine and error checks
            _connectionString = args[0];
            _tableSchema = args[1] ?? "dbo";
            _tableName = args[2];
            _sqlQueries = new SqlQueries(_tableSchema, _tableName);

            CreateTableAndIndexes();
        }

        private void CreateTableAndIndexes()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    var command = new SqlCommand(_sqlQueries.CreateTable, connection, transaction);
                    command.ExecuteNonQuery();

                    command = new SqlCommand(
                        _sqlQueries.CreateNonClusteredIndexOnExpirationTime,
                        connection,
                        transaction);
                    command.ExecuteNonQuery();

                    transaction.Commit();
                }
            }
        }
    }
}