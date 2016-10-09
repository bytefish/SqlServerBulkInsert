// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data;
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
    public class StreamingDataReader<TEntity> : IDataReader
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

        public void Close()
        {
            Dispose();
        }


        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            return sourceEnumerator.MoveNext();
        }

        public void Dispose()
        {
            sourceEnumerator.Dispose();
        }

        public int FieldCount
        {
            get { return columns.Length; }
        }

        public string TableName
        {
            get { return table.TableName; }
        }

        public string SchemaName
        {
            get { return table.Schema; }
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotSupportedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotSupportedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotSupportedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotSupportedException();
        }

        public double GetDouble(int i)
        {
            throw new NotSupportedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotSupportedException();
        }

        public float GetFloat(int i)
        {
            throw new NotSupportedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            return columns[i].ColumnName;
        }

        public int GetOrdinal(string name)
        {
            return ordinalLookupTable[name];
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public object this[int i]
        {
            get { throw new NotImplementedException(); }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }
        
        object IDataRecord.GetValue(int i)
        {
            return columns[i].GetValue(sourceEnumerator.Current);
        }

        public int Depth { get; private set; }

        public bool IsClosed { get; private set; }

        public int RecordsAffected { get; private set; }
    }
}
