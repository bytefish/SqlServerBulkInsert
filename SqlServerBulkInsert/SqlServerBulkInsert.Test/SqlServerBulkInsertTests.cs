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

        List<TestEntity> entities = Enumerable.Range(1, 10).Select(i => new TestEntity(
            Guid.NewGuid(),
            $"User {i}",
            20 + i,
            1000.50m * i,
            i % 2 == 0,
            DateTime.Now,
            DateTimeOffset.Now
        )).ToList();

        Assert.IsNotNull(_connection);

        await writer.WriteAllAsync(_connection, "dbo", "TestBulkTable", entities);

        using SqlCommand cmd = _connection.CreateCommand();
        
        cmd.CommandText = "SELECT COUNT(*) FROM dbo.TestBulkTable";
        
        var count = (int?)await cmd.ExecuteScalarAsync();
        
        Assert.AreEqual(10, count);
    }

    [TestMethod]
    public async Task WriteAllAsync_ShouldThrowException_OnPrimaryKeyViolation()
    {
        Guid duplicateId = Guid.NewGuid();

        List<TestEntity> entities = new List<TestEntity>
            {
                new TestEntity(duplicateId, "First", 30, 100, true, DateTime.Now, DateTimeOffset.Now),
                new TestEntity(duplicateId, "Second", 30, 100, true, DateTime.Now, DateTimeOffset.Now)
            };

        SqlServerBulkWriter<TestEntity> writer = new SqlServerBulkWriter<TestEntity>(Mapper);

        Assert.IsNotNull(_connection);
        
        await Assert.ThrowsAsync<SqlException>(() => writer.WriteAllAsync(_connection, "dbo", "TestBulkTable", entities));
    }
}