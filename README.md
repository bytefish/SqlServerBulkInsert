# SqlServerBulkInsert: SQL Server Bulk Copy for .NET ##

SqlServerBulkInsert is a lightweight, database-centric library for performing high-speed bulk inserts into Microsoft SQL Server. It 
provides a functional, fluent wrapper around the native `SqlBulkCopy` API using a custom `IDataReader` adapter to stream data directly 
from your domain models.

This library follows the design philosophy of my other libraries, moving away from application language-centric abstractions to a 
Database-first approach where the mapping is defined by the SQL types of your destination table.

## Key Features ##

* Stateless Mappers: Define your schema mapping once and reuse it across multiple writers or threads.
* Database-First API: Mapping is driven by SQL Server types (e.g., NVarChar, DateTimeOffset, Int), ensuring correct metadata for the driver.
* Automatic Bracketing: Automatic escaping of schema, table, and column names using [] to prevent conflicts with reserved keywords. Handles optional schemas gracefully.
* Zero-Allocation Streaming: Uses a custom IDataReader bridge to pull data lazily from IEnumerable<T>, keeping memory consumption flat.
* AOT Ready: Fully compatible with Native AOT and trimming in modern .NET.

## Quick Start ##

### 1. Define your Data Model ###

Works seamlessly with C# records, structs, or traditional classes.

```csharp
public record SensorData(
    Guid Id,
    string Name,
    int Temperature,
    decimal SignalStrength,
    DateTimeOffset Timestamp
);
```

### 2. Define your Mapping (Stateless & Thread-Safe) ###

The `SqlServerMapper<T>` defines the structure. It is stateless and should be instantiated once as a singleton.

```csharp
private static readonly SqlServerMapper<SensorData> Mapper = 
    new SqlServerMapper<SensorData>()
        .Map("Id", SqlServerTypes.UniqueIdentifier, x => x.Id)
        .Map("Name", SqlServerTypes.NVarChar, x => x.Name)
        // Automatic handling of primitives and nullable structs
        .Map("Temperature", SqlServerTypes.Int, x => x.Temperature)
        .Map("Signal", SqlServerTypes.Decimal, x => x.SignalStrength)
        .Map("Timestamp", SqlServerTypes.DateTimeOffset, x => x.Timestamp);
```

### 3. Execute the Bulk Insert ###

The `SqlServerBulkWriter<T>` executes the bulk copy operation.

```csharp
public async Task SaveDataAsync(SqlConnection conn, IEnumerable<SensorData> data)
{
    var writer = new SqlServerBulkWriter<SensorData>(Mapper)
        .WithBatchSize(5000);

    await writer.WriteAllAsync(conn, "dbo", "Sensors", data);
}
```
