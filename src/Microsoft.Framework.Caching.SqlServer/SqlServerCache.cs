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
    public class SqlServerCache : IDistributedCache
    {
        internal const int ExpirationTimeMultiplier = 2;

        // Table column names
        private const string CacheItemId = "Id";
        private const string CacheItemValue = "Value";
        private const string ExpiresAtTimeUTC = "ExpiresAtTimeUTC";
        private const string SlidingExpirationInTicks = "SlidingExpirationInTicks";

        private readonly SqlServerCacheOptions _options;
        private readonly ILogger _logger;
        private readonly ISystemClock _systemClock;
        private readonly SqlQueries _sqlQueries;
        private DateTimeOffset _lastExpirationScan;

        //TODO: ADD LOGGING
        public SqlServerCache(IOptions<SqlServerCacheOptions> options, ILoggerFactory loggerFactory)
        {
            _options = options.Options;
            _systemClock = _options.SystemClock;
            _sqlQueries = new SqlQueries(_options.TableName);
            _logger = loggerFactory.CreateLogger<SqlServerCache>();
        }

        public void Connect()
        {
            //TODO:
        }

        public Task ConnectAsync()
        {
            // TODO:
            return Task.FromResult(0);
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

                DateTimeOffset oldExpirationTimeUTC;
                TimeSpan? slidingExpiration = null;
                if (reader.Read())
                {
                    // TODO: cache these oridnals as they are not going to change once the app starts
                    var expiresAtTimeUTCIndex = reader.GetOrdinal(ExpiresAtTimeUTC);
                    var cacheItemValueIndex = reader.GetOrdinal(CacheItemValue);
                    var slidingExpirationInTicksIndex = reader.GetOrdinal(SlidingExpirationInTicks);

                    oldExpirationTimeUTC = reader.GetFieldValue<DateTimeOffset>(expiresAtTimeUTCIndex);
                    cacheItemvalue = reader.GetFieldValue<byte[]>(cacheItemValueIndex);

                    if (!reader.IsDBNull(slidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            reader.GetFieldValue<long>(slidingExpirationInTicksIndex));
                    }
                }
                else
                {
                    return null;
                }

                // Check if the current item has a sliding expiration and is going to expire within the
                // next timeout period(ex:20 minutes), then extend the expiration time.
                if (slidingExpiration.HasValue)
                {
                    if (utcNow >= (oldExpirationTimeUTC - slidingExpiration.Value))
                    {
                        reader.Dispose();

                        command = new SqlCommand(_sqlQueries.UpdateCacheItemExpiration, connection);
                        command.Parameters.AddWithValue(CacheItemId, key);
                        command.Parameters.AddWithValue(
                            ExpiresAtTimeUTC,
                            GetNewExpirationTime(slidingExpiration.Value, utcNow));

                        var recordsAffected = command.ExecuteNonQuery();
                        //TODO: log if records not affected?
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

                DateTimeOffset oldExpirationTimeUTC;
                TimeSpan? slidingExpiration = null;
                if (await reader.ReadAsync())
                {
                    // TODO: cache these oridnals as they are not going to change once the app starts
                    var expiresAtTimeUTCIndex = reader.GetOrdinal(ExpiresAtTimeUTC);
                    var cacheItemValueIndex = reader.GetOrdinal(CacheItemValue);
                    var slidingExpirationInTicksIndex = reader.GetOrdinal(SlidingExpirationInTicks);

                    oldExpirationTimeUTC = await reader.GetFieldValueAsync<DateTimeOffset>(expiresAtTimeUTCIndex);
                    cacheItemvalue = await reader.GetFieldValueAsync<byte[]>(cacheItemValueIndex);

                    if (!await reader.IsDBNullAsync(slidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            await reader.GetFieldValueAsync<long>(slidingExpirationInTicksIndex));
                    }
                }
                else
                {
                    return null;
                }

                // Check if the current item has a sliding expiration and is going to expire within the
                // next timeout period(ex:20 minutes), then extend the expiration time.
                if (slidingExpiration.HasValue)
                {
                    if (utcNow >= (oldExpirationTimeUTC - slidingExpiration.Value))
                    {
                        reader.Dispose();

                        command = new SqlCommand(_sqlQueries.UpdateCacheItemExpiration, connection);
                        command.Parameters.AddWithValue(CacheItemId, key);
                        command.Parameters.AddWithValue(
                            ExpiresAtTimeUTC,
                            GetNewExpirationTime(slidingExpiration.Value, utcNow));

                        var recordsAffected = await command.ExecuteNonQueryAsync();
                        //TODO: log if records not affected?
                    }
                }
            }

            StartScanForExpiredCacheItems();

            return cacheItemvalue;
        }

        public void Refresh([NotNull] string key)
        {
            var value = Get(key);
        }

        public async Task RefreshAsync([NotNull] string key)
        {
            var value = await GetAsync(key);
        }

        public void Remove([NotNull] string key)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(_sqlQueries.DeleteCacheItem, connection);
                command.Parameters.AddWithValue(CacheItemId, key);

                connection.Open();

                var effectedRowCount = command.ExecuteNonQuery();
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

                var effectedRowCount = await command.ExecuteNonQueryAsync();
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

                    if(expirationInfo.SlidingExpiration.HasValue)
                    {
                        command.Parameters.AddWithValue(
                            SlidingExpirationInTicks,
                            expirationInfo.SlidingExpiration.Value.Ticks);
                    }
                    else
                    {
                        command.Parameters.AddWithValue(SlidingExpirationInTicks, DBNull.Value);
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
                _logger.LogError("Error deleting expired cache items", ex);
                //TODO: should we throw exception here...if yes, will it be caught as this is a background thread
            }
            finally
            {
                connection.Close();
            }
        }

        private ExpirationInfo GetExpirationInfo(DateTimeOffset utcNow, DistributedCacheEntryOptions options)
        {
            var result = new ExpirationInfo();
            if (options.SlidingExpiration.HasValue)
            {
                var slidingExpiration = options.SlidingExpiration.Value;
                result.SlidingExpiration = slidingExpiration;
                result.ExpiresAtTimeUTC = GetNewExpirationTime(slidingExpiration, utcNow);
            }
            else
            {
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

                if (!absoluteExpiration.HasValue)
                {
                    throw new InvalidOperationException("Either absolute or sliding expiration needs " +
                        "to be provided.");
                }

                result.ExpiresAtTimeUTC = absoluteExpiration.Value;
            }

            return result;
        }

        private DateTimeOffset GetNewExpirationTime(TimeSpan slidingExpiration, DateTimeOffset utcNow)
        {
            return utcNow + TimeSpan.FromTicks(ExpirationTimeMultiplier * slidingExpiration.Ticks);
        }

        private class ExpirationInfo
        {
            // This should never be null. This value is calculated based on Sliding or Absolute expiration
            public DateTimeOffset ExpiresAtTimeUTC { get; set; }

            public TimeSpan? SlidingExpiration { get; set; }
        }
    }
}