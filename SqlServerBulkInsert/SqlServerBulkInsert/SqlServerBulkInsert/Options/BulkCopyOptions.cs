// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;

namespace SqlServerBulkInsert.Options
{
    public class BulkCopyOptions
    {
        
        public readonly int BatchSize;
        public readonly TimeSpan BulkCopyTimeOut;
        public readonly bool EnableStreaming;
        public readonly SqlBulkCopyOptions SqlBulkCopyOptions;

        public BulkCopyOptions(int batchSize, TimeSpan bulkCopyTimeOut, bool enableStreaming, SqlBulkCopyOptions sqlBulkCopyOptions)
        {
            BatchSize = batchSize;
            BulkCopyTimeOut = bulkCopyTimeOut;
            EnableStreaming = enableStreaming;
            SqlBulkCopyOptions = sqlBulkCopyOptions;
        }

        public BulkCopyOptions()
            : this(70000, TimeSpan.FromSeconds(30), true, SqlBulkCopyOptions.Default)
        {
        }
    }
}
