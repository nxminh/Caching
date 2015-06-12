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
    public class SqlServerCacheTest
    {
        private const string TableName = "ASPNET5Cache";

        private readonly string ConnectionString
            = "Server=.;Database=ASPNET5CacheTest;Trusted_Connection=True;";

        [Fact]
        public async Task ReturnsNullValue_ForNonExistingCacheItem()
        {
            // Arrange
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());

            // Act
            var value = await sqlServerCache.GetAsync("NonExisting");

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task SetWithAbsoluteExpirationSetInThePast_Throws()
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromSeconds(10);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act & Assert

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                return sqlServerCache.SetAsync(
                    key,
                    expectedValue,
                    new DistributedCacheEntryOptions().SetAbsoluteExpiration(testClock.UtcNow.AddHours(-1)));
            });
            Assert.Equal("The absolute expiration value must be in the future.", exception.Message);
        }

        [Fact]
        public async Task SetWithSlidingExpiration_ReturnsNullValue_ForExpiredCacheItem()
        {
            // Arrange
            var testClock = new TestClock();
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromSeconds(10)));

            // set the clock's UtcNow far in future
            testClock.Add(TimeSpan.FromHours(10));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task SetWithAbsoluteExpirationRelativeToNow_ReturnsNullValue_ForExpiredCacheItem()
        {
            // Arrange
            var testClock = new TestClock();
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions().SetAbsoluteExpiration(relative: TimeSpan.FromSeconds(10)));

            // set the clock's UtcNow far in future
            testClock.Add(TimeSpan.FromHours(10));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task SetWithAbsoluteExpiration_ReturnsNullValue_ForExpiredCacheItem()
        {
            // Arrange
            var testClock = new TestClock();
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions()
                .SetAbsoluteExpiration(absolute: testClock.UtcNow.Add(TimeSpan.FromSeconds(30))));

            // set the clock's UtcNow far in future
            testClock.Add(TimeSpan.FromHours(10));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task ThrowsException_OnNoSlidingOrAbsoluteExpirationOptions()
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromSeconds(10);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act & Assert
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                return sqlServerCache.SetAsync(
                    key,
                    expectedValue,
                    new DistributedCacheEntryOptions());
            });
            Assert.Equal("Either absolute or sliding expiration needs to be provided.", exception.Message);
        }

        [Fact]
        public async Task DoesNotThrowException_WhenOnlyAbsoluteExpirationSupplied_AbsoluteExpirationRelativeToNow()
        {
            // Arrange
            var testClock = new TestClock();
            var absoluteExpirationRelativeToUtcNow = TimeSpan.FromSeconds(10);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions()
                .SetAbsoluteExpiration(relative: absoluteExpirationRelativeToUtcNow));

            // Assert
            var cacheItemInfo = await GetCacheItemFromDatabase(key);
            Assert.NotNull(cacheItemInfo);
            Assert.Equal(expectedValue, cacheItemInfo.Value);
            Assert.Equal(testClock.Add(absoluteExpirationRelativeToUtcNow).UtcNow, cacheItemInfo.ExpiresAtTimeUTC);
        }

        [Fact]
        public async Task DoesNotThrowException_WhenOnlyAbsoluteExpirationSupplied_AbsoluteExpiration()
        {
            // Arrange
            var testClock = new TestClock();
            var absoluteExpiration = new DateTimeOffset(2025, 1, 1, 1, 0, 0, TimeSpan.Zero);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions()
                .SetAbsoluteExpiration(absolute: absoluteExpiration));

            // Assert
            var cacheItemInfo = await GetCacheItemFromDatabase(key);
            Assert.NotNull(cacheItemInfo);
            Assert.Equal(expectedValue, cacheItemInfo.Value);
            Assert.Equal(absoluteExpiration, cacheItemInfo.ExpiresAtTimeUTC);
        }

        [Fact]
        public async Task ExtendsExpirationTime_ForSlidingExpiration()
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromSeconds(10);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions().SetSlidingExpiration(slidingExpiration));
            // modify the 'UtcNow' to fall in the window which
            // causes the expiration time to be extended
            testClock.Add(TimeSpan.FromSeconds(15));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.NotNull(value);
            Assert.Equal(expectedValue, value);

            // verify if the expiration time in database is set as expected
            var cacheItemInfo = await GetCacheItemFromDatabase(key);
            Assert.NotNull(cacheItemInfo);
            Assert.Equal(
                testClock.Add(
                    TimeSpan.FromSeconds(SqlServerCache.ExpirationTimeMultiplier * slidingExpiration.TotalSeconds))
                    .UtcNow,
                cacheItemInfo.ExpiresAtTimeUTC);
        }

        [Fact]
        public async Task DoestNotExtendsExpirationTime_ForAbsoluteExpiration()
        {
            // Arrange
            var testClock = new TestClock();
            var absoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30);
            var expectedExpiresAtTimeUTC = testClock.UtcNow.Add(absoluteExpirationRelativeToNow);
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions().SetAbsoluteExpiration(absoluteExpirationRelativeToNow));
            testClock.Add(TimeSpan.FromSeconds(25));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.NotNull(value);
            Assert.Equal(expectedValue, value);

            // verify if the expiration time in database is set as expected
            var cacheItemInfo = await GetCacheItemFromDatabase(key);
            Assert.NotNull(cacheItemInfo);
            Assert.Equal(expectedExpiresAtTimeUTC, cacheItemInfo.ExpiresAtTimeUTC);
        }

        [Fact]
        public async Task DeletesCacheItem_OnExplicitlyCalled()
        {
            // Arrange
            var testClock = new TestClock();
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = ConnectionString,
                TableName = TableName,
                SystemClock = testClock,
                ExpirationScanFrequency = TimeSpan.FromHours(2)
            };
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = new SqlServerCache(options, new LoggerFactory().AddConsole());
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromSeconds(10)));

            // Act
            await sqlServerCache.RemoveAsync(key);

            // Assert
            var cacheItemInfo = await GetCacheItemFromDatabase(key);
            Assert.Null(cacheItemInfo);
        }

        private async Task<CacheItemInfo> GetCacheItemFromDatabase(string key)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                var command = new SqlCommand(
                    $"SELECT Id, Value, ExpiresAtTimeUTC, SlidingExpirationInTicks  from {TableName} where Id = @Id",
                    connection);
                command.Parameters.AddWithValue("Id", key);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    var cacheItemInfo = new CacheItemInfo();
                    cacheItemInfo.Id = key;
                    cacheItemInfo.Value = await reader.GetFieldValueAsync<byte[]>(1);
                    cacheItemInfo.ExpiresAtTimeUTC = await reader.GetFieldValueAsync<DateTimeOffset>(2);
                    if (!await reader.IsDBNullAsync(3))
                    {
                        cacheItemInfo.SlidingExpirationInTicks = await reader.GetFieldValueAsync<long>(3);
                    }

                    return cacheItemInfo;
                }
                else
                {
                    return null;
                }
            }
        }
    }
}
