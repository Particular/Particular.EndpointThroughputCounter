namespace Particular.ThroughputQuery.SqlTransport
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;
    using Microsoft.Data.SqlClient;

    public class DatabaseDetails
    {
        string connectionString;
        int successCount;

        public string DatabaseName { get; }
        public List<TableDetails> Tables { get; private set; }
        public int ErrorCount { get; private set; }

        public DatabaseDetails(string connectionString)
        {
            this.connectionString = connectionString;

            try
            {
                var builder = new SqlConnectionStringBuilder { ConnectionString = connectionString };
                DatabaseName = (builder["Initial Catalog"] as string) ?? (builder["Database"] as string);
            }
            catch (Exception x) when (x is FormatException or ArgumentException)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, "There's something wrong with the SQL connection string and it could not be parsed.", x);
            }
        }

        public int TableCount => Tables.Count;

        /// <remarks>
        /// Error numbers caught here:
        /// -2146893019: When you don't add TrustServerCertificate=true to your connection string. We fix this and retry.
        /// 233: Named pipes: No process is on the other end of the pipe
        /// 18456: Login failed
        /// 53: A network-related or instance-specific error occurred while establishing a connection to SQL Server
        /// </remarks>
        public async Task TestConnection(CancellationToken cancellationToken = default)
        {
            try
            {
                try
                {
                    using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
                    {
                        _ = await conn.ExecuteScalarAsync<string>("select @@SERVERNAME").ConfigureAwait(false);
                    }
                }
                catch (SqlException x) when (x.Number == -2146893019)
                {
                    var builder = new SqlConnectionStringBuilder(connectionString);
                    if (builder.TrustServerCertificate)
                    {
                        throw;
                    }

                    builder.TrustServerCertificate = true;
                    connectionString = builder.ToString();
                    using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
                    {
                        _ = await conn.ExecuteScalarAsync<string>("select @@SERVERNAME").ConfigureAwait(false);
                    }
                }
            }
            catch (SqlException x) when (x.Number is 233 or 18456 or 53)
            {
                throw new QueryException(QueryFailureReason.Auth, "Could not access SQL database. Is the connection string correct?", x);
            }
        }

        public async Task GetTables(CancellationToken cancellationToken = default)
        {
            List<TableDetails> tables = null;

            using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                tables = (await conn.QueryAsync<TableDetails>(GetQueueListCommandText).ConfigureAwait(false)).ToList();
            }

            _ = tables.RemoveAll(t => IgnoreTable(t.TableName));
            foreach (var table in tables)
            {
                table.Database = this;
            }

            Tables = tables;
        }

        public async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            return conn;
        }

        public void LogSuccessOrFailure(bool success, bool reset = false)
        {
            if (reset)
            {
                ErrorCount = 0;
                successCount = 0;
            }
            else if (success)
            {
                successCount++;
                if (successCount > 5)
                {
                    ErrorCount = 0;
                }
            }
            else
            {
                ErrorCount++;
            }
        }

        bool IgnoreTable(string tableName)
        {
            if (tableName is "error" or "audit")
            {
                return true;
            }

            if (tableName.EndsWith(".Timeouts", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (tableName.EndsWith(".TimeoutsDispatcher", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (tableName.StartsWith("Particular.", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Query works by finidng all the columns in any table that *could* be from an NServiceBus
        /// queue table, grouping by schema+name, and then using the HAVING COUNT(*) = 8 clause
        /// to ensure that all 8 columns are represented. Delay tables, for example, will match
        /// on 3 of the columns (Headers, Body, RowVersion) and many user tables might have an
        /// Id column, but the HAVING clause filters these out.
        /// </summary>
        const string GetQueueListCommandText = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

SELECT C.TABLE_SCHEMA as TableSchema, C.TABLE_NAME as TableName
FROM [INFORMATION_SCHEMA].[COLUMNS] C
WHERE
    (C.COLUMN_NAME = 'Id' AND C.DATA_TYPE = 'uniqueidentifier') OR
    (C.COLUMN_NAME = 'CorrelationId' AND C.DATA_TYPE = 'varchar') OR
    (C.COLUMN_NAME = 'ReplyToAddress' AND C.DATA_TYPE = 'varchar') OR
    (C.COLUMN_NAME = 'Recoverable' AND C.DATA_TYPE = 'bit') OR
    (C.COLUMN_NAME = 'Expires' AND C.DATA_TYPE = 'datetime') OR
    (C.COLUMN_NAME = 'Headers') OR
    (C.COLUMN_NAME = 'Body' AND C.DATA_TYPE = 'varbinary') OR
    (C.COLUMN_NAME = 'RowVersion' AND C.DATA_TYPE = 'bigint')
GROUP BY C.TABLE_SCHEMA, C.TABLE_NAME
HAVING COUNT(*) = 8";
    }
}