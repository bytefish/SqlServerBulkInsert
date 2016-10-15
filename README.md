# SqlServerBulkInsert #

[SqlServerBulkInsert](https://github.com/bytefish/SqlServerBulkInsert) is a library for efficient bulk inserts to SQL Server databases. 

It wraps the [SqlBulkCopy](https://msdn.microsoft.com/de-de/library/system.data.sqlclient.sqlbulkcopy(v=vs.110).aspx) class behind a nice Fluent API.

## Benchmark Results ##

[Benchmark]: https://github.com/bytefish/SqlServerBulkInsert/blob/master/SqlServerBulkInsert/SqlServerBulkInsert/SqlServerBulkInsert.Test/Integration/BatchSizeIntegrationTest.cs

The [Benchmark] bulk writes 1000000 entities to an SQL Server database and measures the elapsed time.

```
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 10000, Streaming = True)] Elapsed Time = 00:00:10.68
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 50000, Streaming = True)] Elapsed Time = 00:00:11.62
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 80000, Streaming = True)] Elapsed Time = 00:00:12.76
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 100000, Streaming = True)] Elapsed Time = 00:00:11.67
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 10000, Streaming = False)] Elapsed Time = 00:00:11.03
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 50000, Streaming = False)] Elapsed Time = 00:00:11.64
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 80000, Streaming = False)] Elapsed Time = 00:00:11.54
[BatchExperiment (NumberOfEntities = 1000000, BatchSize = 100000, Streaming = False)] Elapsed Time = 00:00:12.54
```

## Example ##

```csharp
// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using NUnit.Framework;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using SqlServerBulkInsert.Mapping;
using SqlServerBulkInsert.Test.Base;

namespace SqlServerBulkInsert.Test.Mapping
{
    [TestFixture]
    public class BulkCopyTest : TransactionalTestBase
    {
        /// <summary>
        /// The strongly entity, which is going to be inserted.
        /// </summary>
        private class TestEntity
        {
            public Int32 Int32 { get; set; }
            public String String { get; set; }
        }

        /// <summary>
        /// Holds a TableDefinition and can return the full qualified name.
        /// </summary>
        private class TableDefintion
        {
            public readonly string SchemaName;
            public readonly string TableName;

            public TableDefintion(string schemaName, string tableName)
            {
                SchemaName = schemaName;
                TableName = tableName;
            }

            public string GetFullQualifiedName() 
            {
                return string.Format("[{0}].[{1}]", SchemaName, TableName);
            }
        }

        private class TestEntityMapping : AbstractMap<TestEntity>
        {
            public TestEntityMapping()
                : base("UnitTest", "BulkInsertSample")
            {
                Map("ColInt32", x => x.Int32);
                Map("ColString", x => x.String);
            }
        }

        /// <summary>
        /// The table definition used in the unit test.
        /// </summary>
        private TableDefintion tableDefinition;

        /// <summary>
        /// The SqlServerBulkInsert, which will be tested.
        /// </summary>
        private SqlServerBulkInsert<TestEntity> subject;

        protected override void OnSetupInTransaction()
        {
            tableDefinition = new TableDefintion("UnitTest", "BulkInsertSample");

            subject = new SqlServerBulkInsert<TestEntity>(new TestEntityMapping());

        }

        [Test]
        public void SmallIntMappingTest()
        {
            // Create the Table:
            CreateTable(tableDefinition);

            // Create the Test Data:
            var entity0 = new TestEntity()
            {
                Int32 = 10,
                String = "Hello World"
            };

            var entity1 = new TestEntity()
            {
                Int32 = 20,
                String = "Hello World 2.0"
            };

            // Save the test data as Bulk:
            subject.Write(connection, transaction, new[] { entity0, entity1 });

            // Check if we have inserted the correct amount of rows:
            Assert.AreEqual(2, GetRowCount(tableDefinition));

            // Now get all results and order them by their Int32 value:
            var orderedResults = GetAll(tableDefinition).OrderBy(x => x.Int32).ToArray();

            // And assert the result:
            Assert.AreEqual(10, orderedResults[0].Int32);
            Assert.AreEqual("Hello World", orderedResults[0].String);

            Assert.AreEqual(20, orderedResults[1].Int32);
            Assert.AreEqual("Hello World 2.0", orderedResults[1].String);

        }
     
        private int CreateTable(TableDefintion tableDefinition)
        {
            string cmd = string.Format("CREATE TABLE {0}(ColInt32 int, ColString varchar(50));", tableDefinition.GetFullQualifiedName());

            using (var sqlCommand = new SqlCommand(cmd))
            {
                sqlCommand.Connection = connection;
                sqlCommand.Transaction = transaction;

                return sqlCommand.ExecuteNonQuery();
            }
        }

        private int GetRowCount(TableDefintion tableDefinition)
        {
            string cmd = string.Format("SELECT COUNT(*) FROM {0};", tableDefinition.GetFullQualifiedName());
            using (var sqlCommand = new SqlCommand(cmd))
            {
                sqlCommand.Connection = connection;
                sqlCommand.Transaction = transaction;

                return (Int32) sqlCommand.ExecuteScalar();
            }
        }
        
        private List<TestEntity> GetAll(TableDefintion tableDefinition)
        {
            var results = new List<TestEntity>();
            
            using (var reader = GetAllRaw(tableDefinition))
            {
                while (reader.Read())
                {
                    results.Add(new TestEntity
                    {
                        Int32 = reader.GetInt32(reader.GetOrdinal("ColInt32")),
                        String = reader.GetString(reader.GetOrdinal("ColString"))
                    });
                }
            }

            return results;
        }

        private SqlDataReader GetAllRaw(TableDefintion tableDefinition)
        {
            string cmd = string.Format("SELECT * FROM {0};", tableDefinition.GetFullQualifiedName());
            using (var sqlCommand = new SqlCommand(cmd))
            {
                sqlCommand.Connection = connection;
                sqlCommand.Transaction = transaction;

                return sqlCommand.ExecuteReader();
            }
        }
    }
}
```

