// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlServerBulkInsert.Model;
using System;
using System.Collections.Generic;

namespace SqlServerBulkInsert.Mapping
{
    public abstract class AbstractMap<TEntity>
    {
        public readonly TableDefinition Table;

        public readonly List<ColumnDefinition<TEntity>> Columns;

        public AbstractMap(string tableName)
            : this(string.Empty, tableName)
        {
        }

        public AbstractMap(string schemaName, string tableName)
        {
            Table = new TableDefinition
            {
                Schema = schemaName,
                TableName = tableName
            };

            Columns = new List<ColumnDefinition<TEntity>>();
        }

        protected void Map<TProperty>(string columnName, Func<TEntity, TProperty> propertyGetter)
        {
            Columns.Add(new ColumnDefinition<TEntity, TProperty>(columnName, propertyGetter));
        }
    }
}
