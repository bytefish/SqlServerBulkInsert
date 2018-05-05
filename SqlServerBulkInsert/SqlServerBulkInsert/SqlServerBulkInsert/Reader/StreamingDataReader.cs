// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using SqlServerBulkInsert.Model;

namespace SqlServerBulkInsert.Reader
{
    public static class ColumnDefinitionUtils
    {
        public static IDictionary<string, int> BuildLookupTable<TEntity>(ColumnDefinition<TEntity>[] columns)
        {
            var ordinalLookupTable = new Dictionary<string, int>();

            for (int ordinalIndex = 0; ordinalIndex < columns.Length; ordinalIndex++)
            {
                ordinalLookupTable[columns[ordinalIndex].ColumnName] = ordinalIndex;
            }

            return ordinalLookupTable;
        }
    }

    /// <summary>
    /// A simple implementation of a DataReader used for running an SqlBulkCopy.
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public class StreamingDataReader<TEntity> : DbDataReader
    {
        private readonly TableDefinition table;

        private readonly ColumnDefinition<TEntity>[] columns;
        
        private readonly IEnumerator<TEntity> sourceEnumerator;

        private readonly IDictionary<string, int> ordinalLookupTable;

        public StreamingDataReader(IEnumerable<TEntity> source, TableDefinition table, ColumnDefinition<TEntity>[] columns)
        {
            this.table = table;
            this.columns = columns;
            this.sourceEnumerator = source.GetEnumerator();
            this.ordinalLookupTable = ColumnDefinitionUtils.BuildLookupTable(columns);           
        }

        public override void Close()
        {
            DisposeEnumerator();
        }
        
        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            return sourceEnumerator.MoveNext();
        }

        public void DisposeEnumerator()
        {
            sourceEnumerator.Dispose();
        }

        public override int FieldCount
        {
            get { return columns.Length; }
        }

        public override bool HasRows { get; }

        public override bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public override  byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public override char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }
        

        public override string GetDataTypeName(int i)
        {
            throw new NotSupportedException();
        }

        public override DateTime GetDateTime(int i)
        {
            throw new NotSupportedException();
        }

        public override decimal GetDecimal(int i)
        {
            throw new NotSupportedException();
        }

        public override double GetDouble(int i)
        {
            throw new NotSupportedException();
        }

        public override IEnumerator GetEnumerator()
        {
            return sourceEnumerator;
        }

        public override Type GetFieldType(int i)
        {
            throw new NotSupportedException();
        }

        public override float GetFloat(int i)
        {
            throw new NotSupportedException();
        }

        public override Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public override int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public override long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int i)
        {
            return columns[i].ColumnName;
        }

        public override int GetOrdinal(string name)
        {
            return ordinalLookupTable[name];
        }

        public override string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int i)
        {
            var value = columns[i].GetValue(sourceEnumerator.Current);

            return value == null;
        }

        public override object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public override object this[int i]
        {
            get { throw new NotImplementedException(); }
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override object GetValue(int i)
        {
            return columns[i].GetValue(sourceEnumerator.Current);
        }

        public override int Depth { get; }

        public override bool IsClosed { get; }

        public override int RecordsAffected { get; }
    }
}
