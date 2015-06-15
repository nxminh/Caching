// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Caching.SqlServer;
using Microsoft.Framework.Internal;

namespace Microsoft.Framework.DependencyInjection
{
    public static class SqlServerCacheExtensions
    {
        public static IServiceCollection AddSqlServerCache([NotNull] this IServiceCollection services)
        {
            services.AddOptions();
            services.AddSingleton<IDistributedCache, SqlServerCache>();
            return services;
        }

        public static void ConfigureSqlServerCache(
            [NotNull] this IServiceCollection services,
            [NotNull] Action<SqlServerCacheOptions> options)
        {
            services.Configure(options);
        }
    }
}