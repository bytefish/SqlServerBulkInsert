using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            : this(10000, TimeSpan.FromSeconds(30), true, SqlBulkCopyOptions.Default)
        {
        }
    }
}
