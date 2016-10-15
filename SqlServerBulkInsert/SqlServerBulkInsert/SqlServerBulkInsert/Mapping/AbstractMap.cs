using SqlServerBulkInsert.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
