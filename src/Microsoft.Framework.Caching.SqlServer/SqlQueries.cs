// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class SqlQueries
    {
        private const string CreateTableFormat = "CREATE TABLE [{0}](" +
            "Id nvarchar(100)  NOT NULL PRIMARY KEY, " +
            "Value varbinary(MAX) NOT NULL, " +
            "ExpiresAtTimeUTC datetimeoffset NOT NULL, " +
            "SlidingExpirationInTicks bigint NULL," +
            "AbsoluteExpiration datetimeoffset NULL)";

        private const string CreateNonClusteredIndexOnExpirationTimeFormat
            = "CREATE NONCLUSTERED INDEX Index_ExpiresAtTimeUTC ON [{0}](ExpiresAtTimeUTC)";

        // Used for getting table schema when trying to 'Connect' to the database
        private const string GetTableSchemaFormat = "SELECT * FROM [{0}]";

        private const string GetCacheItemFormat =
            "SELECT Id, Value, ExpiresAtTimeUTC, SlidingExpirationInTicks, AbsoluteExpiration " +
            "FROM [{0}] WHERE Id = @Id AND @UtcNow <= ExpiresAtTimeUTC";

        private const string AddCacheItemFormat = "INSERT INTO [{0}] " +
            "(Id, Value, ExpiresAtTimeUTC, SlidingExpirationInTicks, AbsoluteExpiration) " +
            "VALUES (@Id, @Value, @ExpiresAtTimeUTC, @SlidingExpirationInTicks, @AbsoluteExpiration)";

        private const string UpdateCacheItemFormat = "UPDATE [{0}] SET Value = @Value, " +
            "ExpiresAtTimeUTC = @ExpiresAtTimeUTC  WHERE Id = @Id";

        private const string DeleteCacheItemFormat = "DELETE FROM [{0}] WHERE Id = @Id";

        private const string UpdateCacheItemExpirationFormat = "UPDATE [{0}] SET ExpiresAtTimeUTC = @ExpiresAtTimeUTC " +
            "WHERE Id = @Id";

        public const string DeleteExpiredCacheItemsFormat = "DELETE FROM [{0}] WHERE @UtcNow > ExpiresAtTimeUTC";

        public SqlQueries(string tableName)
        {
            //TODO: error checks

            CreateTable = string.Format(CreateTableFormat, tableName);
            CreateNonClusteredIndexOnExpirationTime = string.Format(
                CreateNonClusteredIndexOnExpirationTimeFormat,
                tableName);
            GetTableSchema = string.Format(GetTableSchemaFormat, tableName);
            GetCacheItem = string.Format(GetCacheItemFormat, tableName);
            AddCacheItem = string.Format(AddCacheItemFormat, tableName);
            UpdateCacheItem = string.Format(UpdateCacheItemFormat, tableName);
            DeleteCacheItem = string.Format(DeleteCacheItemFormat, tableName);
            UpdateCacheItemExpiration = string.Format(UpdateCacheItemExpirationFormat, tableName);
            DeleteExpiredCacheItems = string.Format(DeleteExpiredCacheItemsFormat, tableName);
        }

        public string CreateTable { get; }

        public string CreateNonClusteredIndexOnExpirationTime { get; }

        public string GetTableSchema { get; }

        public string GetCacheItem { get; }

        public string AddCacheItem { get; }

        public string UpdateCacheItem { get; }

        public string DeleteCacheItem { get; }

        public string UpdateCacheItemExpiration { get; }

        public string DeleteExpiredCacheItems { get; }
    }
}
