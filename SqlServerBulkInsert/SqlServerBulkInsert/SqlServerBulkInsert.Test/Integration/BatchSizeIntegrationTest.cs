using NUnit.Framework;
using SqlServerBulkInsert.Mapping;
using SqlServerBulkInsert.Options;
using SqlServerBulkInsert.Test.Measurement;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerBulkInsert.Test.Integration
{
    [TestFixture]
    public class BatchSizeIntegrationTest
    {
        /// <summary>
        /// The strongly entity, which is going to be inserted.
        /// </summary>
        public class TestEntity
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
        private TableDefintion tableDefinition = new TableDefintion("UnitTest", "BulkInsertSample");

        [Test]
        public void BatchSizeTest()
        {
            // Check if Streaming makes a difference:
            var streamingModes = new bool[] { true, false };

            // Check if Batch Sizes make a difference:
            var batchSizes = new int[] { 10000, 50000, 80000, 100000 };
            
            foreach (var streamingMode in streamingModes)
            {
                foreach (var batchSize in batchSizes)
                {
                    // Number Of Entities:
                    var numberOfEntities = 1000000;

                    // Bulk Options:
                    var bulkOptions = new BulkCopyOptions(batchSize, TimeSpan.FromSeconds(30), streamingMode, SqlBulkCopyOptions.Default);

                    // Build the Test Subject:
                    var bulkInsert = new SqlServerBulkInsert<TestEntity>(new TestEntityMapping(), bulkOptions);

                    // Experiment Name:
                    var experimentName = string.Format("BatchExperiment (BatchSize = {0}, Streaming = {1})", batchSize, streamingMode);

                    // Measure and Print the Elapsed Time:
                    MeasurementUtils.MeasureElapsedTime(experimentName, () => WriteDataInTransaction(bulkInsert, GenerateEntities(numberOfEntities)));
                }
            }
        }

        public void WriteDataInTransaction(ISqlServerBulkInsert<TestEntity> bulkInsert, IEnumerable<TestEntity> data)
        {
            // Open a new 
            using (var connection = new SqlConnection("Data Source=localhost\\SQLEXPRESS;Integrated Security=true;Initial Catalog=DbUnitTest;"))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    CreateTable(tableDefinition, connection, transaction);

                    bulkInsert.Write(connection, transaction, data);
                }
            }
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


        private int CreateTable(TableDefintion tableDefinition, SqlConnection connection, SqlTransaction transaction)
        {
            string cmd = string.Format("CREATE TABLE {0}(ColInt32 int, ColString varchar(50));", tableDefinition.GetFullQualifiedName());

            using (var sqlCommand = new SqlCommand(cmd))
            {
                sqlCommand.Connection = connection;
                sqlCommand.Transaction = transaction;

                return sqlCommand.ExecuteNonQuery();
            }
        }
    }
}