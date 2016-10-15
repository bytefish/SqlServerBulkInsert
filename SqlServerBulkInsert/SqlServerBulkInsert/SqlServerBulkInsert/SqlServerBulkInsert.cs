// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Data.SqlClient;
using SqlServerBulkInsert.Reader;
using SqlServerBulkInsert.Mapping;
using SqlServerBulkInsert.Options;

namespace SqlServerBulkInsert
{
    public class SqlServerBulkInsert<TEntity> : ISqlServerBulkInsert<TEntity>
    {
        public readonly AbstractMap<TEntity> Mapping;
        public readonly BulkCopyOptions Options;

        public SqlServerBulkInsert(AbstractMap<TEntity> mapping)
            : this(mapping, new BulkCopyOptions())
        {
        }

        public SqlServerBulkInsert(AbstractMap<TEntity> mapping, BulkCopyOptions options)
        {
            Mapping = mapping;
            Options = options;
        }

        public void Write(SqlConnection connection, IEnumerable<TEntity> entities)
        {
            using (var streamingDataReader = new StreamingDataReader<TEntity>(entities, Mapping.Table, Mapping.Columns.ToArray()))
            {
                using (var sqlBulkCopy = new SqlBulkCopy(connection))
                {
                    sqlBulkCopy.BatchSize = Options.BatchSize;
                    sqlBulkCopy.EnableStreaming = Options.EnableStreaming;
                    sqlBulkCopy.BulkCopyTimeout = (int) Options.BulkCopyTimeOut.TotalSeconds;
                    
                    // Set the Destination Table:
                    sqlBulkCopy.DestinationTableName = Mapping.Table.GetFullQualifiedTableName();

                    // Build the Internal Mapping Table:
                    foreach (var column in Mapping.Columns)
                    {
                        sqlBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }

                    // And finally write to the SQL Server:
                    sqlBulkCopy.WriteToServer(streamingDataReader);
                }
            }
        }

        public void Write(SqlConnection connection, SqlTransaction transaction, IEnumerable<TEntity> entities)
        {
            using (var streamingDataReader = new StreamingDataReader<TEntity>(entities, Mapping.Table, Mapping.Columns.ToArray()))
            {
                using (var sqlBulkCopy = new SqlBulkCopy(connection, Options.SqlBulkCopyOptions, transaction))
                {
                    sqlBulkCopy.BatchSize = Options.BatchSize;
                    sqlBulkCopy.EnableStreaming = Options.EnableStreaming;
                    sqlBulkCopy.BulkCopyTimeout = (int)Options.BulkCopyTimeOut.TotalSeconds;

                    // Set the Destination Table:
                    sqlBulkCopy.DestinationTableName = Mapping.Table.GetFullQualifiedTableName();

                    // Build the Internal Mapping Table:
                    foreach (var column in Mapping.Columns)
                    {
                        sqlBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }

                    // And finally write to the SQL Server:
                    sqlBulkCopy.WriteToServer(streamingDataReader);
                }
            }
        }

    }
}