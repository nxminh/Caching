// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Internal;
using Microsoft.Framework.Logging;
using Microsoft.Framework.OptionsModel;

namespace Microsoft.Framework.Caching.SqlServer
{
    /// <summary>
    /// Distributed cache implementation using Microsoft SQL Server database.
    /// </summary>
    public class SqlServerCache : IDistributedCache
    {
        /// <summary>
        /// To prevent frequent updates to a cache item's expiration time in database, especially when doing a
        /// Get operation, a cache item's expiration time is extended by doubling the sliding expiration.
        /// The 'SlidingExpiration' here should be interpreted as the minimum time to live for a cache item before
        /// it expires.
        /// Example:
        /// For a sliding expiration of 30 mins, a new cache item would have its 'ExpiresAtTimeUTC' 60 mins(30 * 2)
        /// from now. All 'Get' operations before the first 30 mins do not cause any database updates to the cache
        /// item's expiration time and any 'Get' operations between the 30th and 60th minute would cause a database
        /// update where the expiration time is again extended by 60 mins.
        /// </summary>
        internal const int ExpirationTimeMultiplier = 2;

        // Table column names
        private const string CacheItemId = "Id";
        private const string CacheItemValue = "Value";
        private const string ExpiresAtTimeUTC = "ExpiresAtTimeUTC";
        private const string SlidingExpirationInTicks = "SlidingExpirationInTicks";
        private const string AbsoluteExpiration = "AbsoluteExpiration";
        private int _cacheItemIdIndex = -1;
        private int _cacheItemValueIndex = -1;
        private int _expiresAtTimeUTCIndex = -1;
        private int _slidingExpirationInTicksIndex = -1;
        private int _absoluteExpirationIndex = -1;

        private readonly SqlServerCacheOptions _options;
        private readonly ILogger _logger;
        private readonly ISystemClock _systemClock;
        private readonly SqlQueries _sqlQueries;
        private DateTimeOffset _lastExpirationScan;

        public SqlServerCache(IOptions<SqlServerCacheOptions> options, ILoggerFactory loggerFactory)
        {
            _options = options.Options;
            _systemClock = _options.SystemClock;
            _sqlQueries = new SqlQueries(_options.TableName);
            _logger = loggerFactory.CreateLogger<SqlServerCache>();
        }

        public void Connect()
        {
            // Try connecting to the database to test if its available and also
            // get and cache the ordinal positions of the columns for better performance.
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(_sqlQueries.GetTableSchema, connection);
                connection.Open();

                var reader = command.ExecuteReader(CommandBehavior.SchemaOnly);

                _cacheItemIdIndex = reader.GetOrdinal(CacheItemId);
                _cacheItemValueIndex = reader.GetOrdinal(CacheItemValue);
                _expiresAtTimeUTCIndex = reader.GetOrdinal(ExpiresAtTimeUTC);
                _slidingExpirationInTicksIndex = reader.GetOrdinal(SlidingExpirationInTicks);
                _absoluteExpirationIndex = reader.GetOrdinal(AbsoluteExpiration);
            }
        }

        public async Task ConnectAsync()
        {
            // Try connecting to the database to test if its available and also
            // get and cache the ordinal positions of the columns for better performance.
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(_sqlQueries.GetTableSchema, connection);
                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly);

                _cacheItemIdIndex = reader.GetOrdinal(CacheItemId);
                _cacheItemValueIndex = reader.GetOrdinal(CacheItemValue);
                _expiresAtTimeUTCIndex = reader.GetOrdinal(ExpiresAtTimeUTC);
                _slidingExpirationInTicksIndex = reader.GetOrdinal(SlidingExpirationInTicks);
                _absoluteExpirationIndex = reader.GetOrdinal(AbsoluteExpiration);
            }
        }

        public byte[] Get([NotNull] string key)
        {
            byte[] cacheItemvalue = null;
            var utcNow = _systemClock.UtcNow;

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(_sqlQueries.GetCacheItem, connection);
                command.Parameters.AddWithValue(CacheItemId, key);
                command.Parameters.AddWithValue("UtcNow", utcNow);

                connection.Open();

                var reader = command.ExecuteReader(CommandBehavior.SingleRow);

                DateTimeOffset oldExpirationTime;
                TimeSpan? slidingExpiration = null;
                DateTimeOffset? absoluteExpiration = null;
                if (reader.Read())
                {
                    oldExpirationTime = reader.GetFieldValue<DateTimeOffset>(_expiresAtTimeUTCIndex);
                    cacheItemvalue = reader.GetFieldValue<byte[]>(_cacheItemValueIndex);

                    if (!reader.IsDBNull(_slidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            reader.GetFieldValue<long>(_slidingExpirationInTicksIndex));
                    }

                    if (!reader.IsDBNull(_absoluteExpirationIndex))
                    {
                        absoluteExpiration = reader.GetFieldValue<DateTimeOffset>(_absoluteExpirationIndex);
                    }
                }
                else
                {
                    return null;
                }

                // Check if the current item has a sliding expiration and is going to expire within the
                // next timeout period(ex:20 minutes), then extend the expiration time.
                if (slidingExpiration.HasValue &&
                    utcNow >= (oldExpirationTime - slidingExpiration.Value))
                {
                    var newExpirationTime = GetExpirationTime(slidingExpiration, absoluteExpiration, utcNow);

                    if (oldExpirationTime != newExpirationTime)
                    {
                        reader.Dispose();

                        command = new SqlCommand(_sqlQueries.UpdateCacheItemExpiration, connection);
                        command.Parameters.AddWithValue(CacheItemId, key);
                        command.Parameters.AddWithValue(ExpiresAtTimeUTC, newExpirationTime);

                        command.ExecuteNonQuery();
                    }
                }
            }

            StartScanForExpiredCacheItems();

            return cacheItemvalue;
        }

        public async Task<byte[]> GetAsync([NotNull] string key)
        {
            byte[] cacheItemvalue = null;
            var utcNow = _systemClock.UtcNow;

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(_sqlQueries.GetCacheItem, connection);
                command.Parameters.AddWithValue(CacheItemId, key);
                command.Parameters.AddWithValue("UtcNow", utcNow);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

                DateTimeOffset oldExpirationTime;
                TimeSpan? slidingExpiration = null;
                DateTimeOffset? absoluteExpiration = null;
                if (await reader.ReadAsync())
                {
                    oldExpirationTime = await reader.GetFieldValueAsync<DateTimeOffset>(_expiresAtTimeUTCIndex);
                    cacheItemvalue = await reader.GetFieldValueAsync<byte[]>(_cacheItemValueIndex);

                    if (!await reader.IsDBNullAsync(_slidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            await reader.GetFieldValueAsync<long>(_slidingExpirationInTicksIndex));
                    }

                    if (!reader.IsDBNull(_absoluteExpirationIndex))
                    {
                        absoluteExpiration = await reader.GetFieldValueAsync<DateTimeOffset>(_absoluteExpirationIndex);
                    }
                }
                else
                {
                    return null;
                }

                // Check if the current item has a sliding expiration and is going to expire within the
                // next timeout period(ex:20 minutes), then extend the expiration time.
                if (slidingExpiration.HasValue &&
                    utcNow >= (oldExpirationTime - slidingExpiration.Value))
                {
                    var newExpirationTime = GetExpirationTime(slidingExpiration, absoluteExpiration, utcNow);

                    if (oldExpirationTime != newExpirationTime)
                    {
                        reader.Dispose();

                        command = new SqlCommand(_sqlQueries.UpdateCacheItemExpiration, connection);
                        command.Parameters.AddWithValue(CacheItemId, key);
                        command.Parameters.AddWithValue(ExpiresAtTimeUTC, newExpirationTime);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }

            StartScanForExpiredCacheItems();

            return cacheItemvalue;
        }

        public void Refresh([NotNull] string key)
        {
            Get(key);
        }

        public async Task RefreshAsync([NotNull] string key)
        {
            await GetAsync(key);
        }

        public void Remove([NotNull] string key)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(_sqlQueries.DeleteCacheItem, connection);
                command.Parameters.AddWithValue(CacheItemId, key);

                connection.Open();

                command.ExecuteNonQuery();
            }

            StartScanForExpiredCacheItems();
        }

        public async Task RemoveAsync([NotNull] string key)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(_sqlQueries.DeleteCacheItem, connection);
                command.Parameters.AddWithValue(CacheItemId, key);

                await connection.OpenAsync();

                await command.ExecuteNonQueryAsync();
            }

            StartScanForExpiredCacheItems();
        }

        public void Set([NotNull] string key, [NotNull] byte[] value, [NotNull] DistributedCacheEntryOptions options)
        {
            var utcNow = _systemClock.UtcNow;
            var expirationInfo = GetExpirationInfo(utcNow, options);

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                // Try updating the cache item and if it does not succeed then its probably a new item
                var command = new SqlCommand(_sqlQueries.UpdateCacheItem, connection);
                command.Parameters.AddWithValue(CacheItemId, key);
                command.Parameters.AddWithValue(CacheItemValue, value);
                command.Parameters.AddWithValue(ExpiresAtTimeUTC, expirationInfo.ExpiresAtTimeUTC);

                connection.Open();

                var recordsAffected = command.ExecuteNonQuery();

                // only add if update didn't affect any records
                if (recordsAffected <= 0)
                {
                    command = new SqlCommand(_sqlQueries.AddCacheItem, connection);
                    command.Parameters.AddWithValue(CacheItemId, key);
                    command.Parameters.AddWithValue(CacheItemValue, value);
                    command.Parameters.AddWithValue(ExpiresAtTimeUTC, expirationInfo.ExpiresAtTimeUTC);

                    if (expirationInfo.SlidingExpiration.HasValue)
                    {
                        command.Parameters.AddWithValue(
                            SlidingExpirationInTicks,
                            expirationInfo.SlidingExpiration.Value.Ticks);
                    }
                    else
                    {
                        command.Parameters.AddWithValue(SlidingExpirationInTicks, DBNull.Value);
                    }

                    if (expirationInfo.AbsoluteExpiration.HasValue)
                    {
                        command.Parameters.AddWithValue(
                            AbsoluteExpiration,
                            expirationInfo.AbsoluteExpiration.Value);
                    }
                    else
                    {
                        command.Parameters.AddWithValue(AbsoluteExpiration, DBNull.Value);
                    }

                    recordsAffected = command.ExecuteNonQuery();
                }
            }

            StartScanForExpiredCacheItems();
        }

        public async Task SetAsync(
            [NotNull] string key,
            [NotNull] byte[] value,
            [NotNull] DistributedCacheEntryOptions options)
        {
            var utcNow = _systemClock.UtcNow;
            var expirationInfo = GetExpirationInfo(utcNow, options);

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                // Try updating the cache item and if it does not succeed then its probably a new item
                var command = new SqlCommand(_sqlQueries.UpdateCacheItem, connection);
                command.Parameters.AddWithValue(CacheItemId, key);
                command.Parameters.AddWithValue(CacheItemValue, value);
                command.Parameters.AddWithValue(ExpiresAtTimeUTC, expirationInfo.ExpiresAtTimeUTC);

                await connection.OpenAsync();

                var recordsAffected = await command.ExecuteNonQueryAsync();

                // only add if update didn't affect any records
                if (recordsAffected <= 0)
                {
                    command = new SqlCommand(_sqlQueries.AddCacheItem, connection);
                    command.Parameters.AddWithValue(CacheItemId, key);
                    command.Parameters.AddWithValue(CacheItemValue, value);
                    command.Parameters.AddWithValue(ExpiresAtTimeUTC, expirationInfo.ExpiresAtTimeUTC);

                    if (expirationInfo.SlidingExpiration.HasValue)
                    {
                        command.Parameters.AddWithValue(
                            SlidingExpirationInTicks,
                            expirationInfo.SlidingExpiration.Value.Ticks);
                    }
                    else
                    {
                        command.Parameters.AddWithValue(SlidingExpirationInTicks, DBNull.Value);
                    }

                    if (expirationInfo.AbsoluteExpiration.HasValue)
                    {
                        command.Parameters.AddWithValue(
                            AbsoluteExpiration,
                            expirationInfo.AbsoluteExpiration.Value);
                    }
                    else
                    {
                        command.Parameters.AddWithValue(AbsoluteExpiration, DBNull.Value);
                    }

                    recordsAffected = await command.ExecuteNonQueryAsync();
                }
            }

            StartScanForExpiredCacheItems();
        }

        // Called by multiple actions to see how long it's been since we last checked for expired items.
        // If sufficient time has elapsed then a scan is initiated on a background task.
        private void StartScanForExpiredCacheItems()
        {
            var utcNow = _systemClock.UtcNow;
            if ((utcNow - _lastExpirationScan) > _options.ExpirationScanFrequency)
            {
                _lastExpirationScan = utcNow;
                ThreadPool.QueueUserWorkItem(DeleteExpiredCacheItems, state: utcNow);
            }
        }

        // to enable unit testing
        internal void DeleteExpiredCacheItems(object state)
        {
            var utcNow = (DateTimeOffset)state;
            var connection = new SqlConnection(_options.ConnectionString);
            var command = new SqlCommand(_sqlQueries.DeleteExpiredCacheItems, connection);
            command.Parameters.AddWithValue("UtcNow", utcNow);

            try
            {
                connection.Open();

                var effectedRowCount = command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                _logger.LogError("An error occurred while deleting expired cache items.", ex);
            }
            finally
            {
                connection.Close();
            }
        }

        private ExpirationInfo GetExpirationInfo(DateTimeOffset utcNow, DistributedCacheEntryOptions options)
        {
            var result = new ExpirationInfo();
            result.SlidingExpiration = options.SlidingExpiration;

            // calculate absolute expiration
            DateTimeOffset? absoluteExpiration = null;
            if (options.AbsoluteExpirationRelativeToNow.HasValue)
            {
                absoluteExpiration = utcNow.Add(options.AbsoluteExpirationRelativeToNow.Value);
            }
            else if (options.AbsoluteExpiration.HasValue)
            {
                if (options.AbsoluteExpiration <= utcNow)
                {
                    throw new InvalidOperationException("The absolute expiration value must be in the future.");
                }

                absoluteExpiration = options.AbsoluteExpiration.Value;
            }
            result.AbsoluteExpiration = absoluteExpiration;

            result.ExpiresAtTimeUTC = GetExpirationTime(result.SlidingExpiration, result.AbsoluteExpiration, utcNow);

            return result;
        }

        private DateTimeOffset GetExpirationTime(
            TimeSpan? slidingExpiration,
            DateTimeOffset? absoluteExpiration,
            DateTimeOffset utcNow)
        {
            if (!slidingExpiration.HasValue && !absoluteExpiration.HasValue)
            {
                throw new InvalidOperationException("Either absolute or sliding expiration needs " +
                    "to be provided.");
            }

            if (slidingExpiration.HasValue)
            {
                // if there is also an absolute expiration, then the sliding expiration extension should
                // not exceed the absolute expiration.
                var newSlidingExpirationTime = utcNow + TimeSpan.FromTicks(
                    ExpirationTimeMultiplier * slidingExpiration.Value.Ticks);
                if (absoluteExpiration.HasValue && newSlidingExpirationTime > absoluteExpiration.Value)
                {
                    return absoluteExpiration.Value;
                }

                return newSlidingExpirationTime;
            }

            return absoluteExpiration.Value;
        }

        private class ExpirationInfo
        {
            // This should never be null. This value is calculated based on Sliding or Absolute expiration
            public DateTimeOffset ExpiresAtTimeUTC { get; set; }

            public TimeSpan? SlidingExpiration { get; set; }

            public DateTimeOffset? AbsoluteExpiration { get; set; }
        }
    }
}