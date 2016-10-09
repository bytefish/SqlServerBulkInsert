// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace SqlServerBulkInsert.Model
{
    public abstract class ColumnDefinition<TEntityType>
    {
        public readonly string ColumnName;

        public ColumnDefinition(string columnName)
        {
            ColumnName = columnName;
        }

        public abstract object GetValue(TEntityType entity);
    }

    public class ColumnDefinition<TEntityType, TPropertyType> : ColumnDefinition<TEntityType>
    {
        public readonly string ColumnName;
        public readonly Func<TEntityType, TPropertyType> ValueGetter;

        public ColumnDefinition(string columnName, Func<TEntityType, TPropertyType> valueGetter) 
            : base(columnName)
        {
            ValueGetter = valueGetter;
        }

        public override object GetValue(TEntityType entity)
        {
            return ValueGetter(entity);
        }

        public override string ToString()
        {
            return string.Format("ColumnMapping (ColumnIndex = {0})", ColumnName);
        }
    }
}
