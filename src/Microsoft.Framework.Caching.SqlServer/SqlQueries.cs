// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.Framework.Caching.SqlServer
{
    internal static class SqlQueries
    {
        public const string GetSession = "SELECT Id, Value, ExpiresAtTimeUTC " +
            "FROM Sessions WHERE Id = @Id AND ExpiresAtTimeUTC >= @UtcNow";

        public const string AddSession = "INSERT INTO Sessions (Id, Value, ExpiresAtTimeUTC) " +
            "VALUES (@Id, @Value, @ExpiresAtTimeUTC)";

        public const string UpdateSession = "UPDATE Sessions SET Value = @Value, " +
            "ExpiresAtTimeUTC = @ExpiresAtTimeUTC  WHERE Id = @Id";

        public const string DeleteSession = "DELETE FROM Sessions WHERE Id = @Id";

        public const string UpdateSessionExpiration = "UPDATE Sessions SET ExpiresAtTimeUTC = @ExpiresAtTimeUTC " +
            "WHERE Id = @Id";

        public const string DeleteExpiredSessions = "DELETE FROM Sessions WHERE ExpiresAtTimeUTC < @UtcNow";
    }
}
