using NUnit.Framework;
using SqlServerBulkInsert.Mapping;
using SqlServerBulkInsert.Options;
using SqlServerBulkInsert.Test.Base;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerBulkInsert.Test.Integration
{
    [TestFixture]
    public class BenchmarkIntegrationTest : TransactionalTestBase
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
        }
        
        [Test]
        public void OneMillionEntitiesTest()
        {
            // Bulk Options:
            var bulkOptions = new BulkCopyOptions(70000, TimeSpan.FromSeconds(30), true, SqlBulkCopyOptions.Default);

            // Build the Test Subject:
            var subject = new SqlServerBulkInsert<TestEntity>(new TestEntityMapping(), bulkOptions);

            // Create the Table:
            CreateTable(tableDefinition);

            // One Million Entities:
            var numberOfEntities = 1000000;

            // Create the Enumerable Test Data:
            var data = GenerateEntities(numberOfEntities);

            // Save the test data as Bulk:
            subject.Write(connection, transaction, data);

            // Check if we have inserted the correct amount of rows:
            Assert.AreEqual(numberOfEntities, GetRowCount(tableDefinition));
        }

        private IEnumerable<TestEntity> GenerateEntities(int numberOfEntities)
        {
            return Enumerable.Range(0, numberOfEntities)
                .Select(x => new TestEntity
                {
                    Int32 = x,
                    String = string.Format("Value {0}", x)
                });
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

                return (Int32)sqlCommand.ExecuteScalar();
            }
        }
    }
}
