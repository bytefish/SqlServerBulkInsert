using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SqlServerBulkInsert;

/// <summary>
/// A Delegate that extracts a column value from an entity. This is used to map properties of the entity to columns 
/// in the SQL Server table.
/// </summary>
public delegate object SqlServerColumnExtractor<TEntity>(TEntity entity);

/// <summary>
/// The SqlServerType class represents a SQL Server data type and its associated metadata (like precision and scale for numeric types).
/// </summary>
public class SqlServerType<TValue>
{
    public SqlDbType DbType { get; }
    public int Precision { get; }
    public int Scale { get; }

    public SqlServerType(SqlDbType dbType, int precision = 0, int scale = 0)
    {
        DbType = dbType;
        Precision = precision;
        Scale = scale;
    }
}

/// <summary>
/// The Mapping between an entity property and a SQL Server column. It includes the SQL data type, precision, scale, and 
/// a delegate to extract the value from the entity.
/// </summary>
public record SqlServerColumnMapping<TEntity>(
    SqlDbType DbType,
    int Precision,
    int Scale,
    SqlServerColumnExtractor<TEntity> Extractor
);

/// <summary>
/// SQL Server data types that can be used in the mapping. This class provides a convenient way to specify 
/// the SQL data type and its metadata when defining column mappings.
/// </summary>
public static class SqlServerTypes
{
    public static readonly SqlServerType<bool> Bit = new(SqlDbType.Bit);
    public static readonly SqlServerType<byte> TinyInt = new(SqlDbType.TinyInt);
    public static readonly SqlServerType<short> SmallInt = new(SqlDbType.SmallInt);
    public static readonly SqlServerType<int> Int = new(SqlDbType.Int);
    public static readonly SqlServerType<long> BigInt = new(SqlDbType.BigInt);
    public static readonly SqlServerType<float> Real = new(SqlDbType.Real);
    public static readonly SqlServerType<double> Float = new(SqlDbType.Float);
    public static readonly SqlServerType<decimal> Money = new(SqlDbType.Money);
    public static readonly SqlServerType<decimal> SmallMoney = new(SqlDbType.SmallMoney);
    public static readonly SqlServerType<decimal> Decimal = new(SqlDbType.Decimal, 38, 10);

    public static readonly SqlServerType<string> VarChar = new(SqlDbType.VarChar, -1);
    public static readonly SqlServerType<string> NVarChar = new(SqlDbType.NVarChar, -1);
    public static readonly SqlServerType<byte[]> VarBinary = new(SqlDbType.VarBinary, -1);

    public static readonly SqlServerType<Guid> UniqueIdentifier = new(SqlDbType.UniqueIdentifier);

    public static readonly SqlServerType<DateTime> Date = new(SqlDbType.Date);
    public static readonly SqlServerType<TimeSpan> Time = new(SqlDbType.Time);
    public static readonly SqlServerType<DateTime> DateTime = new(SqlDbType.DateTime);
    public static readonly SqlServerType<DateTime> DateTime2 = new(SqlDbType.DateTime2);
    public static readonly SqlServerType<DateTimeOffset> DateTimeOffset = new(SqlDbType.DateTimeOffset);
}

/// <summary>
/// A Thread-safe builder for mapping an entity type to a SQL Server table. It allows you to define 
/// how properties of the entity
/// </summary>
public class SqlServerMapper<TEntity>
{
    private readonly List<(string Name, SqlServerColumnMapping<TEntity> Mapping)> _columns = new();

    public SqlServerMapper()
    {
    }

    /// <summary>
    /// Maps a column using a SQL type and an extractor.
    /// C# handles primitives and boxing automatically.
    /// </summary>
    public SqlServerMapper<TEntity> Map<TValue>(string columnName, SqlServerType<TValue> type, Func<TEntity, TValue> extractor)
    {
        SqlServerColumnMapping<TEntity> mapping = new(
            type.DbType,
            type.Precision,
            type.Scale,
            e => (object)extractor(e)! ?? DBNull.Value);

        _columns.Add((columnName, mapping));
        return this;
    }

    /// <summary>
    /// Overload for Nullable Structs (e.g., int?, Guid?).
    /// </summary>
    public SqlServerMapper<TEntity> Map<TValue>(string columnName, SqlServerType<TValue?> type, Func<TEntity, TValue?> extractor) where TValue : struct
    {
        SqlServerColumnMapping<TEntity> mapping = new(
            type.DbType,
            type.Precision,
            type.Scale,
            e => (object)extractor(e)! ?? DBNull.Value);

        _columns.Add((columnName, mapping));

        return this;
    }

    internal IReadOnlyList<(string Name, SqlServerColumnMapping<TEntity> Mapping)> GetMappings() => _columns;
}



/// <summary>
/// A DataReader Implementation that adapts an IEnumerable<TEntity> to be consumed by SqlBulkCopy. It uses the 
/// mappings defined in the SqlServerMapper
/// </summary>
internal class SqlServerBulkDataAdapter<TEntity> : IDataReader
{
    private readonly IEnumerator<TEntity> _iterator;
    private readonly IReadOnlyList<(string Name, SqlServerColumnMapping<TEntity> Mapping)> _mappings;

    private TEntity? _current;

    public SqlServerBulkDataAdapter(IEnumerable<TEntity> entities, SqlServerMapper<TEntity> mapper)
    {
        _iterator = entities.GetEnumerator();
        _mappings = mapper.GetMappings();
    }

    public bool Read()
    {
        if (_iterator.MoveNext())
        {
            _current = _iterator.Current;
            return true;
        }
        return false;
    }

    public object GetValue(int i) => _mappings[i].Mapping.Extractor(_current!);

    public int GetValues(object[] values)
    {
        int numToCopy = Math.Min(values.Length, FieldCount);

        for (int i = 0; i < numToCopy; i++)
        {
            values[i] = GetValue(i);
        }

        return numToCopy;
    }

    public int FieldCount => _mappings.Count;

    public string GetName(int i) => _mappings[i].Name;

    public void Close() => _iterator.Dispose();

    public void Dispose() => _iterator.Dispose();

    public int Depth => 0;

    public bool IsClosed => false;

    public int RecordsAffected => -1;

    public bool GetBoolean(int i) => (bool)GetValue(i);

    public byte GetByte(int i) => (byte)GetValue(i);

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int buffOffset, int length) => 0;

    public char GetChar(int i) => (char)GetValue(i);

    public long GetChars(int i, long fieldOffset, char[]? buffer, int buffOffset, int length) => 0;

    public IDataReader GetData(int i) => null!;

    public string GetDataTypeName(int i) => _mappings[i].Mapping.DbType.ToString();

    public DateTime GetDateTime(int i) => (DateTime)GetValue(i);

    public decimal GetDecimal(int i) => (decimal)GetValue(i);

    public double GetDouble(int i) => (double)GetValue(i);

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public Type GetFieldType(int i) => typeof(object);

    public float GetFloat(int i) => (float)GetValue(i);

    public Guid GetGuid(int i) => (Guid)GetValue(i);

    public short GetInt16(int i) => (short)GetValue(i);

    public int GetInt32(int i) => (int)GetValue(i);

    public long GetInt64(int i) => (long)GetValue(i);

    public string GetString(int i) => (string)GetValue(i);

    public int GetOrdinal(string name) => _mappings.ToList().FindIndex(m => m.Name == name);

    public DataTable GetSchemaTable() => null!;

    public bool IsDBNull(int i) => GetValue(i) == null || GetValue(i) == DBNull.Value;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    public bool NextResult() => false;
}

/// <summary>
/// The main class that performs the bulk insert operation. It takes a SqlServerMapper to understand 
/// how to map the entity properties to SQL Server columns,
/// </summary>
public class SqlServerBulkWriter<TEntity>
{
    private readonly SqlServerMapper<TEntity> _mapper;

    private int _batchSize = 0;

    private SqlBulkCopyOptions _options = SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.CheckConstraints;

    public SqlServerBulkWriter(SqlServerMapper<TEntity> mapper) => _mapper = mapper;

    public SqlServerBulkWriter<TEntity> WithBatchSize(int size) { _batchSize = size; return this; }

    public SqlServerBulkWriter<TEntity> WithOptions(SqlBulkCopyOptions options) { _options = options; return this; }

    /// <summary>
    /// Asynchronously executes the bulk insert.
    /// </summary>
    public async Task WriteAllAsync(SqlConnection connection, string? schemaName, string tableName, IEnumerable<TEntity> entities, CancellationToken ct = default)
    {
        using SqlBulkCopy bulkCopy = new(connection, _options, null);

        bulkCopy.DestinationTableName = GetDestinationTable(schemaName, tableName);
        bulkCopy.BatchSize = _batchSize;

        foreach ((string Name, SqlServerColumnMapping<TEntity> Mapping) mapping in _mapper.GetMappings())
        {
            bulkCopy.ColumnMappings.Add(mapping.Name, mapping.Name);
        }

        using SqlServerBulkDataAdapter<TEntity> adapter = new(entities, _mapper);

        await bulkCopy.WriteToServerAsync(adapter, ct);
    }

    /// <summary>
    /// Synchronously executes the bulk insert.
    /// </summary>
    public void WriteAll(SqlConnection connection, string? schemaName, string tableName, IEnumerable<TEntity> entities)
    {
        using SqlBulkCopy bulkCopy = new(connection, _options, null);

        bulkCopy.DestinationTableName = GetDestinationTable(schemaName, tableName);
        bulkCopy.BatchSize = _batchSize;

        foreach ((string Name, SqlServerColumnMapping<TEntity> Mapping) mapping in _mapper.GetMappings())
        {
            bulkCopy.ColumnMappings.Add(mapping.Name, mapping.Name);
        }

        using SqlServerBulkDataAdapter<TEntity> adapter = new(entities, _mapper);

        bulkCopy.WriteToServer(adapter);
    }

    private string GetDestinationTable(string? schemaName, string tableName)
    {
        string safeTable = tableName.StartsWith("[") ? tableName : $"[{tableName}]";

        if (string.IsNullOrWhiteSpace(schemaName))
        {
            return safeTable;
        }

        string safeSchema = schemaName.StartsWith("[") ? schemaName : $"[{schemaName}]";

        return $"{safeSchema}.{safeTable}";
    }
}