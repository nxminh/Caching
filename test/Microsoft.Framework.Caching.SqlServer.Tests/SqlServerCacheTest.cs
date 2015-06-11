// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Internal;
using Microsoft.Framework.Logging;
using Xunit;

namespace Microsoft.Framework.Caching.SqlServer
{
    // This requires SQL Server database to be setup
    // public
    class SqlServerCacheTest
    {
        private readonly string ConnectionString
            = "Server=.;Database=ASPNET5SessionStateTest;Trusted_Connection=True;";

        private const int ExpirationTimeMultiplier = 3;

        [Fact]
        public async Task ReturnsNullValue_ForNonExistingSessionId()
        {
            // Arrange
            var options = new SqlServerCacheOptions() { ConnectionString = ConnectionString };
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            var sqlServerCache = new SqlServerCache(options, loggerFactory);

            // Act
            var value = await sqlServerCache.GetAsync("NonExisting");

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task ReturnsNullValue_ForExpiredSession()
        {
            // Arrange
            var testClock = new TestClock();
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                SystemClock = testClock
            };
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, loggerFactory);
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions());

            // set the clock's UtcNow far in future
            testClock.Add(TimeSpan.FromHours(10));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task ReturnsSavedValue_WithExpectedExpirationTime()
        {
            // Arrange
            var testClock = new TestClock();
            var idleTimeout = TimeSpan.FromSeconds(10);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                IdleTimeout = idleTimeout,
                SystemClock = testClock
            };
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, loggerFactory);
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act
            await sqlServerCache.SetAsync(key, expectedValue, new DistributedCacheEntryOptions());

            // Assert
            var sessionInfo = await GetSessionFromDatabase(key);
            Assert.Equal(expectedValue, sessionInfo.Value);
            Assert.Equal(
                testClock.Add(TimeSpan.FromSeconds(ExpirationTimeMultiplier * idleTimeout.TotalSeconds)).UtcNow,
                sessionInfo.ExpiresAtTimeUTC);
        }

        [Fact]
        public async Task ExtendsExpirationTime()
        {
            // Arrange
            var testClock = new TestClock();
            var idleTimeout = TimeSpan.FromSeconds(10);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                IdleTimeout = idleTimeout,
                SystemClock = testClock
            };
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, loggerFactory);
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            await sqlServerCache.SetAsync(key, expectedValue, new DistributedCacheEntryOptions());
            // modify the 'UtcNow' to fall in the window which
            // causes the expiration time to be extended
            testClock.Add(TimeSpan.FromSeconds(25));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.NotNull(value);
            Assert.Equal(expectedValue, value);

            // verify if the expiration time in database is set as expected
            var sessionInfo = await GetSessionFromDatabase(key);
            Assert.Equal(
                testClock.Add(TimeSpan.FromSeconds(ExpirationTimeMultiplier * idleTimeout.TotalSeconds)).UtcNow,
                sessionInfo.ExpiresAtTimeUTC);
        }

        [Fact]
        public async Task DeletesSession_OnExplicitlyCalled()
        {
            // Arrange
            var testClock = new TestClock();
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                SystemClock = testClock
            };
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, loggerFactory);
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions());

            // Act
            await sqlServerCache.RemoveAsync(key);

            // Assert
            var sessionInfo = await GetSessionFromDatabase(key);
            Assert.Null(sessionInfo);
        }

        [Fact]
        public void DeletesExpiredSessions()
        {
            // Arrange
            var testClock = new TestClock();
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                SystemClock = testClock
            };
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, loggerFactory);
            sqlServerCache.Set(key, Encoding.UTF8.GetBytes("Hello, World!"), new DistributedCacheEntryOptions());

            // Act
            // Modify the 'UtcNow' to sometime in future to trigger deletion of expired items.
            var utcNow = testClock.Add(TimeSpan.FromHours(5)).UtcNow;
            sqlServerCache.DeleteExpiredSessions(state: utcNow);

            // Assert
            var sessionInfo = GetSessionFromDatabase(key).GetAwaiter().GetResult();
            Assert.Null(sessionInfo);
        }

        private async Task<SessionInfo> GetSessionFromDatabase(string key)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(
                    "SELECT Id, Value, ExpiresAtTimeUTC from Sessions where Id = @Id",
                    connection);
                command.Parameters.AddWithValue("Id", key);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync();

                if(await reader.ReadAsync())
                {
                    var sessionInfo = new SessionInfo();
                    sessionInfo.Id = key;
                    sessionInfo.Value = (byte[])reader["Value"];
                    sessionInfo.ExpiresAtTimeUTC = (DateTimeOffset)reader["ExpiresAtTimeUTC"];
                    return sessionInfo;
                }
                else
                {
                    return null;
                }
            }
        }
    }
}
