using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SqlServerBulkInsert.Test;

[TestClass]
public class SqlServerBulkInsertIntegrationTests
{
    private const string ConnectionString = "Server=localhost,14330;Database=master;User Id=sa;Password=MyStrongPassw0rd;Encrypt=True;TrustServerCertificate=True;";

    private SqlConnection? _connection;

    [TestInitialize]
    public async Task Setup()
    {
        _connection = new SqlConnection(ConnectionString);

        await _connection.OpenAsync();

        using SqlCommand cmd = _connection.CreateCommand();

        cmd.CommandText = @"
                IF OBJECT_ID('dbo.TestBulkTable', 'U') IS NOT NULL DROP TABLE dbo.TestBulkTable;
                CREATE TABLE dbo.TestBulkTable (
                    Id UNIQUEIDENTIFIER PRIMARY KEY,
                    Name NVARCHAR(255),
                    Age INT,
                    Salary DECIMAL(18, 2),
                    IsActive BIT,
                    CreatedAt DATETIME2,
                    LastModified DATETIMEOFFSET
                );";

        await cmd.ExecuteNonQueryAsync();
    }

    [TestCleanup]
    public async Task Cleanup()
    {
        if (_connection != null)
        {
            using SqlCommand cmd = _connection.CreateCommand();
            cmd.CommandText = "IF OBJECT_ID('dbo.TestBulkTable', 'U') IS NOT NULL DROP TABLE dbo.TestBulkTable;";
            await cmd.ExecuteNonQueryAsync();
            await _connection.DisposeAsync();
        }
    }

    public record TestEntity(
        Guid Id,
        string Name,
        int Age,
        decimal Salary,
        bool IsActive,
        DateTime CreatedAt,
        DateTimeOffset LastModified
    );

    private static readonly SqlServerMapper<TestEntity> Mapper = new SqlServerMapper<TestEntity>()
        .Map("Id", SqlServerTypes.UniqueIdentifier, x => x.Id)
        .Map("Name", SqlServerTypes.NVarChar, x => x.Name)
        .Map("Age", SqlServerTypes.Int, x => x.Age)
        .Map("Salary", SqlServerTypes.Decimal, x => x.Salary)
        .Map("IsActive", SqlServerTypes.Bit, x => x.IsActive)
        .Map("CreatedAt", SqlServerTypes.DateTime2, x => x.CreatedAt)
        .Map("LastModified", SqlServerTypes.DateTimeOffset, x => x.LastModified);

    [TestMethod]
    public async Task WriteAllAsync_ShouldInsertMultipleRecordsSuccessfully()
    {
        SqlServerBulkWriter<TestEntity> writer = new SqlServerBulkWriter<TestEntity>(Mapper)
            .WithBatchSize(100);

        DateTime now = DateTime.Now;
        DateTimeOffset nowOffset = DateTimeOffset.Now;

        List<TestEntity> entities = Enumerable.Range(1, 10).Select(i => new TestEntity(
            Guid.NewGuid(),
            $"User Async {i}",
            20 + i,
            1000.50m * i,
            i % 2 == 0,
            now.AddSeconds(i),
            nowOffset.AddSeconds(i)
        )).ToList();

        Assert.IsNotNull(_connection);
        await writer.WriteAllAsync(_connection, "dbo", "TestBulkTable", entities);

        // Verify Count
        using SqlCommand cmd = _connection.CreateCommand();

        cmd.CommandText = "SELECT COUNT(*) FROM dbo.TestBulkTable";

        int? count = (int?)await cmd.ExecuteScalarAsync();

        Assert.AreEqual(10, count);

        // Verify Data Values
        cmd.CommandText = "SELECT Id, Name, Age, Salary, IsActive, CreatedAt, LastModified FROM dbo.TestBulkTable ORDER BY Age ASC";

        using SqlDataReader reader = await cmd.ExecuteReaderAsync();

        int index = 0;

        while (await reader.ReadAsync())
        {
            TestEntity expected = entities[index];

            Assert.AreEqual(expected.Id, reader.GetGuid(0));
            Assert.AreEqual(expected.Name, reader.GetString(1));
            Assert.AreEqual(expected.Age, reader.GetInt32(2));
            Assert.AreEqual(expected.Salary, reader.GetDecimal(3));
            Assert.AreEqual(expected.IsActive, reader.GetBoolean(4));

            Assert.AreEqual(expected.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss"), reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss"));
            Assert.AreEqual(expected.LastModified.Offset, ((DateTimeOffset)reader.GetValue(6)).Offset);

            index++;
        }

        Assert.AreEqual(10, index, "Should have read 10 rows from the database.");
    }



    [TestMethod]
    public void WriteAll_ShouldInsertMultipleRecordsSuccessfully_Sync()
    {
        SqlServerBulkWriter<TestEntity> writer = new SqlServerBulkWriter<TestEntity>(Mapper)
            .WithBatchSize(100);

        // Create precise timestamps (rounding to avoid potential diffs in ticks vs SQL precision)
        DateTime now = DateTime.Now;
        DateTimeOffset nowOffset = DateTimeOffset.Now;

        List<TestEntity> entities = Enumerable.Range(1, 10).Select(i => new TestEntity(
            Guid.NewGuid(),
            $"User Sync {i}",
            30 + i,
            2000.50m * i,
            true,
            now.AddSeconds(i),
            nowOffset.AddSeconds(i)
        )).ToList();

        Assert.IsNotNull(_connection);
        writer.WriteAll(_connection, "dbo", "TestBulkTable", entities);

        // Verify Count
        using SqlCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM dbo.TestBulkTable";
        int? count = (int?)cmd.ExecuteScalar();
        Assert.AreEqual(10, count);

        // Verify Data Values
        cmd.CommandText = "SELECT Id, Name, Age, Salary, IsActive, CreatedAt, LastModified FROM dbo.TestBulkTable ORDER BY Age ASC";

        using SqlDataReader reader = cmd.ExecuteReader();

        int index = 0;
        while (reader.Read())
        {
            TestEntity expected = entities[index];

            Assert.AreEqual(expected.Id, reader.GetGuid(0));
            Assert.AreEqual(expected.Name, reader.GetString(1));
            Assert.AreEqual(expected.Age, reader.GetInt32(2));
            Assert.AreEqual(expected.Salary, reader.GetDecimal(3));
            Assert.AreEqual(expected.IsActive, reader.GetBoolean(4));

            Assert.AreEqual(expected.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss"), reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss"));
            Assert.AreEqual(expected.LastModified.Offset, ((DateTimeOffset)reader.GetValue(6)).Offset);

            index++;
        }

        Assert.AreEqual(10, index, "Should have read 10 rows from the database.");
    }


    [TestMethod]
    public void WriteAll_ShouldInsertMultipleRecordsSuccessfully()
    {
        SqlServerBulkWriter<TestEntity> writer = new SqlServerBulkWriter<TestEntity>(Mapper)
            .WithBatchSize(100);

        List<TestEntity> entities = Enumerable.Range(1, 10).Select(i => new TestEntity(
            Guid.NewGuid(),
            $"User Sync {i}",
            30 + i,
            2000.50m * i,
            true,
            DateTime.Now,
            DateTimeOffset.Now
        )).ToList();

        Assert.IsNotNull(_connection);
        writer.WriteAll(_connection, "dbo", "TestBulkTable", entities);

        using SqlCommand cmd = _connection.CreateCommand();

        cmd.CommandText = "SELECT COUNT(*) FROM dbo.TestBulkTable";

        int? count = (int?)cmd.ExecuteScalar();

        Assert.AreEqual(10, count);
    }

    [TestMethod]
    public async Task WriteAllAsync_ShouldThrowException_OnPrimaryKeyViolation()
    {
        Guid duplicateId = Guid.NewGuid();

        List<TestEntity> entities = new()
        {
                new TestEntity(duplicateId, "First", 30, 100, true, DateTime.Now, DateTimeOffset.Now),
                new TestEntity(duplicateId, "Second", 30, 100, true, DateTime.Now, DateTimeOffset.Now)
            };

        SqlServerBulkWriter<TestEntity> writer = new(Mapper);

        Assert.IsNotNull(_connection);

        await Assert.ThrowsAsync<SqlException>(() => writer.WriteAllAsync(_connection, "dbo", "TestBulkTable", entities));
    }
}