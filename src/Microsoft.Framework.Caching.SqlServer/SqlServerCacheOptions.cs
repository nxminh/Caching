// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.Framework.Internal;
using Microsoft.Framework.OptionsModel;

namespace Microsoft.Framework.Caching.SqlServer
{
    public class SqlServerCacheOptions : IOptions<SqlServerCacheOptions>
    {
        public ISystemClock SystemClock { get; set; } = new SystemClock();

        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(20);

        public TimeSpan ExpirationScanFrequency { get; set; } = TimeSpan.FromMinutes(10);

        public string ConnectionString { get; set; }

        public SqlServerCacheOptions Options
        {
            get
            {
                return this;
            }
        }

        public SqlServerCacheOptions GetNamedOptions(string name)
        {
            return this;
        }
    }
}