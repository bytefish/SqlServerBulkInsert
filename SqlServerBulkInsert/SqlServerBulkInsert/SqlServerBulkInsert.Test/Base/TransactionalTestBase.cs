// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using NUnit.Framework;
using System.Data.SqlClient;

namespace SqlServerBulkInsert.Test.Base
{
    public class TransactionalTestBase
    {
        protected SqlConnection connection;
        protected SqlTransaction transaction;

        [SetUp]
        public void Setup()
        {
            OnSetupBeforeTransaction();

            connection = new SqlConnection(@"Data Source=.\MSSQLSERVER2017;Integrated Security=true;Initial Catalog=DbUnitTest;");
            connection.Open();

            transaction = connection.BeginTransaction();

            OnSetupInTransaction();
        }


        protected virtual void OnSetupBeforeTransaction()
        {
        }

        protected virtual void OnSetupInTransaction()
        {
        }

        [TearDown]
        protected void TearDown()
        {
            OnTeardownInTransaction();

            transaction.Rollback();
            transaction.Dispose();

            connection.Close();
            connection.Dispose();

            OnTeardownAfterTransaction();
        }

        protected virtual void OnTeardownInTransaction()
        {
        }

        protected virtual void OnTeardownAfterTransaction()
        {
        }

    }
}
