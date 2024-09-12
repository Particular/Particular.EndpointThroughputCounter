namespace Particular.ThroughputQuery.PostgreSql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;

    public class DatabaseDetails
    {
        readonly string connectionString;

        public string DatabaseName { get; }
        public List<QueueTableName> Tables { get; private set; }
        public int ErrorCount { get; private set; }

        public DatabaseDetails(string connectionString)
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder
                {
                    ConnectionString = connectionString
                };
                DatabaseName = builder["Initial Catalog"] as string ?? builder["Database"] as string;
                this.connectionString = builder.ToString();
            }
            catch (Exception x) when (x is FormatException or ArgumentException)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, "There's something wrong with the PostgreSQL connection string and it could not be parsed.", x);
            }
        }

        public int TableCount => Tables.Count;

        public async Task TestConnection(CancellationToken cancellationToken = default)
        {
            try
            {
                await TestGetServerName(cancellationToken).ConfigureAwait(false);
            }
            catch (NpgsqlException x) when (IsConnectionOrLoginIssue(x))
            {
                throw new QueryException(QueryFailureReason.Auth, "Could not access PostgreSQL database. Is the connection string correct?", x);
            }
        }

        static bool IsConnectionOrLoginIssue(NpgsqlException x)
        {
            //TODO
            return x.Message.Contains("Cannot connect to the database");
        }

        async Task TestGetServerName(CancellationToken cancellationToken)
        {
            using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "select @@SERVERNAME";
                _ = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
            }
        }

        public async Task GetTables(CancellationToken cancellationToken = default)
        {
            List<QueueTableName> tables = new();

            using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = GetQueueListCommandText;
                using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (reader.Read())
                    {
                        var schema = reader["TableSchema"] as string;
                        var name = reader["TableName"] as string;
                        tables.Add(new QueueTableName(DatabaseName, schema, name));
                    }
                }
            }

            _ = tables.RemoveAll(t => IgnoreTable(t.Name));

            Tables = tables;
        }

        public async Task<QueueTableSnapshot[]> GetSnapshot(CancellationToken cancellationToken = default)
        {
            var data = Tables.Select(t => new QueueTableSnapshot(t)).ToArray();

            using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
            {
                foreach (var table in data)
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $"select IDENT_CURRENT('{table.FullName}')";
                        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                        if (value is decimal decimalValue) // That's the return type of IDENT_CURRENT
                        {
                            table.RowVersion = (long)decimalValue;
                        }
                    }
                }
            }

            return data;
        }

        public static QueueTableThroughput[] CalculateThroughput(IEnumerable<QueueTableSnapshot> startData, IEnumerable<QueueTableSnapshot> endData)
        {
            return CalculateThroughputInternal(startData, endData)
                .OrderBy(t => t.DisplayName)
                .ToArray();
        }

        static IEnumerable<QueueTableThroughput> CalculateThroughputInternal(IEnumerable<QueueTableSnapshot> startData, IEnumerable<QueueTableSnapshot> endData)
        {
            var dictionary = startData.ToDictionary(t => t.DisplayName);

            foreach (var endSnap in endData)
            {
                if (dictionary.TryGetValue(endSnap.DisplayName, out var startSnap))
                {
                    if (endSnap.RowVersion is not null && startSnap.RowVersion is not null)
                    {
                        var throughput = endSnap.RowVersion.Value - startSnap.RowVersion.Value;
                        yield return new QueueTableThroughput(endSnap, throughput);
                    }
                }
            }
        }

        async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            return conn;
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