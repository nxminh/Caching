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
        private string _tableName;
        private SqlQueries _sqlQueries;

        public void Main(string[] args)
        {
            //TODO1: use CommandLine and error checks
            _connectionString = args[0];
            _tableName = args[1];
            _sqlQueries = new SqlQueries(_tableName);

            CreateTableAndIndexes();
        }

        private void CreateTableAndIndexes()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                try
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
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
    }
}
