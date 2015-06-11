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
        // Table column names
        private const string SessionId = "Id";
        private const string SessionValue = "Value";
        private const string ExpiresAtTimeUTC = "ExpiresAtTimeUTC";

        private readonly SqlServerCacheOptions _options;
        private readonly ILogger _logger;
        private DateTimeOffset _lastExpirationScan;
        private readonly ISystemClock _systemClock;

        //TODO: ADD LOGGING
        public SqlServerCache(IOptions<SqlServerCacheOptions> options, ILoggerFactory loggerFactory)
        {
            _options = options.Options;
            _systemClock = _options.SystemClock;
            _logger = loggerFactory.CreateLogger<SqlServerCache>();
        }

        public void Connect()
        {
            // TODO: this is called when SessionMiddleware starts.
            // Any connection failure here will cause the web app to not start.
            // Should we even have these methods? This causes the apps to be lined up in a particular order.
        }

        public Task ConnectAsync()
        {
            // same as above
            return Task.FromResult(0);
        }

        public byte[] Get([NotNull] string key)
        {
            byte[] sessionvalue = null;
            var utcNow = _systemClock.UtcNow;

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetSession, connection);
                command.Parameters.AddWithValue(SessionId, key);
                command.Parameters.AddWithValue("UtcNow", utcNow);

                connection.Open();

                var sqlDataReader = command.ExecuteReader(CommandBehavior.SingleRow);

                DateTimeOffset oldExpirationTimeUTC;
                if (sqlDataReader.Read())
                {
                    //TODO: GetFieldValueAsync?
                    oldExpirationTimeUTC = DateTimeOffset.Parse(sqlDataReader[ExpiresAtTimeUTC].ToString());
                    sessionvalue = (byte[])sqlDataReader[SessionValue];
                }
                else
                {
                    return null;
                }

                // Check if the session is going to expire within the next timeout period(ex:20 minutes),
                // then extend the expiration time..
                if (utcNow >= oldExpirationTimeUTC.AddSeconds(-1 * _options.IdleTimeout.TotalSeconds))
                {
                    sqlDataReader.Dispose();

                    command = new SqlCommand(SqlQueries.UpdateSessionExpiration, connection);
                    command.Parameters.AddWithValue(SessionId, key);
                    command.Parameters.AddWithValue(ExpiresAtTimeUTC, GetNewExpirationTime(utcNow));

                    var recordsAffected = command.ExecuteNonQuery();
                }
            }

            StartScanForExpiredSessions();

            return sessionvalue;
        }

        public async Task<byte[]> GetAsync([NotNull] string key)
        {
            byte[] sessionvalue = null;
            var utcNow = _systemClock.UtcNow;

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetSession, connection);
                command.Parameters.AddWithValue(SessionId, key);
                command.Parameters.AddWithValue("UtcNow", utcNow);

                await connection.OpenAsync();

                var sqlDataReader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

                DateTimeOffset oldExpirationTimeUTC;
                if (await sqlDataReader.ReadAsync())
                {
                    //TODO: GetFieldValueAsync
                    oldExpirationTimeUTC = DateTimeOffset.Parse(sqlDataReader[ExpiresAtTimeUTC]?.ToString());
                    sessionvalue = (byte[])sqlDataReader[SessionValue];
                }
                else
                {
                    return null;
                }

                // Check if the session is going to expire within the next timeout period(ex:20 minutes),
                // then extend the expiration time.
                if (utcNow >= oldExpirationTimeUTC.AddSeconds(-1 * _options.IdleTimeout.TotalSeconds))
                {
                    sqlDataReader.Dispose();

                    command = new SqlCommand(SqlQueries.UpdateSessionExpiration, connection);
                    command.Parameters.AddWithValue(SessionId, key);
                    command.Parameters.AddWithValue(ExpiresAtTimeUTC, GetNewExpirationTime(utcNow));

                    var recordsAffected = await command.ExecuteNonQueryAsync();
                }
            }

            StartScanForExpiredSessions();

            return sessionvalue;
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
                var command = new SqlCommand(SqlQueries.DeleteSession, connection);
                command.Parameters.AddWithValue(SessionId, key);

                connection.Open();

                var effectedRowCount = command.ExecuteNonQuery();
            }

            StartScanForExpiredSessions();
        }

        public async Task RemoveAsync([NotNull] string key)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.DeleteSession, connection);
                command.Parameters.AddWithValue(SessionId, key);

                await connection.OpenAsync();

                var effectedRowCount = await command.ExecuteNonQueryAsync();
            }

            StartScanForExpiredSessions();
        }

        public void Set([NotNull] string key, [NotNull] byte[] value, DistributedCacheEntryOptions options)
        {
            var utcNow = _systemClock.UtcNow;
            bool sessionAlreadyExists = false;

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetSession, connection);
                command.Parameters.AddWithValue(SessionId, key);
                command.Parameters.AddWithValue("UtcNow", utcNow);

                connection.Open();

                var sqlDataReader = command.ExecuteReader(CommandBehavior.SingleRow);

                if (sqlDataReader.Read())
                {
                    sessionAlreadyExists = sqlDataReader.HasRows;
                }

                // close the reader before opening up another connection
                sqlDataReader.Dispose();

                string query;
                if (sessionAlreadyExists)
                {
                    // Update
                    query = SqlQueries.UpdateSession;
                }
                else
                {
                    // Insert
                    query = SqlQueries.AddSession;
                }

                command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue(SessionId, key);
                command.Parameters.AddWithValue(SessionValue, value);
                command.Parameters.AddWithValue(ExpiresAtTimeUTC, GetNewExpirationTime(utcNow));

                var effectedRowCount = command.ExecuteNonQuery();
            }

            StartScanForExpiredSessions();
        }

        public async Task SetAsync([NotNull] string key, [NotNull] byte[] value, DistributedCacheEntryOptions options)
        {
            var utcNow = _systemClock.UtcNow;
            bool sessionAlreadyExists = false;

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetSession, connection);
                command.Parameters.AddWithValue(SessionId, key);
                command.Parameters.AddWithValue("UtcNow", utcNow);

                await connection.OpenAsync();

                var sqlDataReader = command.ExecuteReader(CommandBehavior.SingleRow);

                if (await sqlDataReader.ReadAsync())
                {
                    sessionAlreadyExists = sqlDataReader.HasRows;
                }

                // close the reader before opening up another connection
                sqlDataReader.Dispose();

                string query;
                if (sessionAlreadyExists)
                {
                    // Update
                    query = SqlQueries.UpdateSession;
                }
                else
                {
                    // Insert
                    query = SqlQueries.AddSession;
                }

                command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue(SessionId, key);
                command.Parameters.AddWithValue(SessionValue, value);
                command.Parameters.AddWithValue(ExpiresAtTimeUTC, GetNewExpirationTime(utcNow));

                var effectedRowCount = await command.ExecuteNonQueryAsync();
            }

            StartScanForExpiredSessions();
        }

        // Called by multiple actions to see how long it's been since we last checked for expired items.
        // If sufficient time has elapsed then a scan is initiated on a background task.
        private void StartScanForExpiredSessions()
        {
            var utcNow = _systemClock.UtcNow;
            if ((utcNow - _lastExpirationScan) > _options.ExpirationScanFrequency)
            {
                _lastExpirationScan = utcNow;
                ThreadPool.QueueUserWorkItem(DeleteExpiredSessions, state: utcNow);
            }
        }

        // to enable unit testing
        internal void DeleteExpiredSessions(object state)
        {
            var utcNow = (DateTimeOffset)state;
            var connection = new SqlConnection(_options.ConnectionString);
            var command = new SqlCommand(SqlQueries.DeleteExpiredSessions, connection);
            command.Parameters.AddWithValue("UtcNow", utcNow);

            try
            {
                connection.Open();

                var effectedRowCount = command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                _logger.LogError("Error deleting expired sessions", ex);
                //TODO: should we throw exception here...if yes, will it be caught as this is a background thread
            }
            finally
            {
                connection.Close();
            }
        }

        private DateTimeOffset GetNewExpirationTime(DateTimeOffset utcNow)
        {
            return utcNow.AddSeconds(3 * _options.IdleTimeout.TotalSeconds);
        }
    }
}