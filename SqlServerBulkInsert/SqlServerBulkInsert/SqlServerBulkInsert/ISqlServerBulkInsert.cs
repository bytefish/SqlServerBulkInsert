// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlServerBulkInsert
{
    public interface ISqlServerBulkInsert<TEntity>
    {
        void Write(SqlConnection connection, IEnumerable<TEntity> entities);

        void Write(SqlConnection connection, SqlTransaction transaction, IEnumerable<TEntity> entities);
    }
}
