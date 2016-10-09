// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using SqlServerBulkInsert.Reader;
using SqlServerBulkInsert.Model;

namespace SqlServerBulkInsert
{
    public class SqlServerBulkInsert<TEntity> : ISqlServerBulkInsert<TEntity>
    {
        
        private TableDefinition Table { get; set; }

        private List<ColumnDefinition<TEntity>> Columns { get; set; }

        public SqlServerBulkInsert(string tableName)
            : this(string.Empty, tableName)
        {
        }

        public SqlServerBulkInsert(string schemaName, string tableName)
        {
            Table = new TableDefinition
            {
                Schema = schemaName,
                TableName = tableName
            };

            Columns = new List<ColumnDefinition<TEntity>>();
        }

        public SqlServerBulkInsert<TEntity> Map<TProperty>(string columnName, Func<TEntity, TProperty> propertyGetter)
        {
            Columns.Add(new ColumnDefinition<TEntity, TProperty>(columnName, propertyGetter));

            return this;
        }

        public void SaveAll(SqlConnection connection, SqlTransaction transaction, IEnumerable<TEntity> entities)
        {
            using (var streamingDataReader = new StreamingDataReader<TEntity>(entities, Table, Columns.ToArray()))
            {
                using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    sqlBulkCopy.DestinationTableName = Table.GetFullQualifiedTableName();

                    foreach (var column in Columns)
                    {
                        sqlBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }

                    sqlBulkCopy.WriteToServer(streamingDataReader);
                }
            }
        }

    }
}