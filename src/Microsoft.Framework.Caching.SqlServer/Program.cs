// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;

namespace Microsoft.Framework.Caching.SqlServer
{
    public class Program
    {
        private const string DefaultDatabaseName = "ASPNET5SessionState";

        public void Main(string[] args)
        {
            if (args.Length == 0)
            {
                throw new ArgumentException("Connection string is required.");
            }

            var connectionString = args[0];

            CreateDatabase(connectionString);

            CreateTableAndIndexes(connectionString);
        }

        private void CreateDatabase(string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                var text = $"CREATE DATABASE [{DefaultDatabaseName}]";

                var command = new SqlCommand(text, connection);
                connection.Open();
                var recordsAffected = command.ExecuteNonQuery();
            }
        }

        private void CreateTableAndIndexes(string connectionString)
        {
            var pairs = connectionString.TrimEnd(';');
            connectionString += $"Database={DefaultDatabaseName};";

            //TODO: execute these in a transaction? but how to execute transaction where if database creation
            // succeeds but others fail? use MultipleActiveResultSets? but is this supported in different
            // database server versions?
            using (var connection = new SqlConnection(connectionString))
            {
                var text = "CREATE TABLE Sessions(" +
                    "Id nvarchar(36)  NOT NULL PRIMARY KEY, " + // Length of Session Id is max 36 characters long
                    "ExpiresAtTimeUTC datetimeoffset NOT NULL, " +
                    "Value varbinary(MAX) NOT NULL)";

                connection.Open();

                var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                try
                {
                    var command = new SqlCommand(text, connection, transaction);
                    command.ExecuteNonQuery();

                    text = "CREATE NONCLUSTERED INDEX Index_ExpiresAtTimeUTC ON Sessions(ExpiresAtTimeUTC)";
                    command = new SqlCommand(text, connection, transaction);
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
