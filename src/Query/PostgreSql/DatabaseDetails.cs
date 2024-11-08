namespace Particular.ThroughputQuery.PostgreSql;

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
            var builder = new NpgsqlConnectionStringBuilder { ConnectionString = connectionString };
            DatabaseName = builder["Database"] as string ?? "postgres";
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
            using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT version()";

                _ = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (NpgsqlException x) when (IsConnectionOrLoginIssue(x))
        {
            throw new QueryException(QueryFailureReason.Auth, "Could not access PostgreSQL database. Is the connection string correct?", x);
        }
    }

    static bool IsConnectionOrLoginIssue(NpgsqlException x) =>
        // Reference is here: https://www.postgresql.org/docs/current/errcodes-appendix.html
        x.SqlState switch
        {
            //28000   invalid_authorization_specification
            //28P01   invalid_password
            "28000" or "28P01" => true,

            //08000   connection_exception
            //08003   connection_does_not_exist
            //08006   connection_failure
            //08001   sqlclient_unable_to_establish_sqlconnection
            //08004   sqlserver_rejected_establishment_of_sqlconnection
            //08007   transaction_resolution_unknown
            //08P01   protocol_violation
            "08000" or "08003" or "08006" or "08001" or "08004" or "08007" or "08P01" => true,

            // Everything else
            _ => false
        };

    public async Task GetTables(CancellationToken cancellationToken = default)
    {
        List<QueueTableName> tables = [];

        using (var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false))
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = GetQueueListCommandText;
            using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
                    cmd.CommandText = $"select last_value from \"{table.SequenceName}\";";
                    var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                    if (value is long longValue)
                    {
                        table.RowVersion = longValue;
                    }
                }
            }
        }

        return data;
    }

    public static QueueTableThroughput[] CalculateThroughput(IEnumerable<QueueTableSnapshot> startData, IEnumerable<QueueTableSnapshot> endData) =>
        CalculateThroughputInternal(startData, endData)
            .OrderBy(t => t.DisplayName)
            .ToArray();

    static IEnumerable<QueueTableThroughput> CalculateThroughputInternal(IEnumerable<QueueTableSnapshot> startData, IEnumerable<QueueTableSnapshot> endData)
    {
        var dictionary = startData.ToDictionary(t => t.DisplayName);

        foreach (var endSnap in endData)
        {
            if (!dictionary.TryGetValue(endSnap.DisplayName, out var startSnap))
            {
                continue;
            }

            if (endSnap.RowVersion is null || startSnap.RowVersion is null)
            {
                continue;
            }

            var throughput = endSnap.RowVersion.Value - startSnap.RowVersion.Value;
            yield return new QueueTableThroughput(endSnap, throughput);
        }
    }

    async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        return conn;
    }

    static bool IgnoreTable(string tableName)
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

        if (tableName is "ServiceControl.ThroughputData")
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Query works by finidng all the columns in any table that *could* be from an NServiceBus
    /// queue table, grouping by schema+name, and then using the HAVING COUNT(*) = 5 clause
    /// to ensure that all 5 columns are represented. Delay tables, for example, will match
    /// on 3 of the columns (Headers, Body, RowVersion) and many user tables might have an
    /// Id column, but the HAVING clause filters these out.
    /// </summary>        /// 
    const string GetQueueListCommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT C.TABLE_SCHEMA as TableSchema, C.TABLE_NAME as TableName
FROM information_schema.columns C
WHERE
    (C.COLUMN_NAME = 'id' AND C.DATA_TYPE = 'uuid') OR
    (C.COLUMN_NAME = 'expires' AND C.DATA_TYPE = 'timestamp without time zone') OR
    (C.COLUMN_NAME = 'headers' AND C.DATA_TYPE = 'text') OR
    (C.COLUMN_NAME = 'body' AND C.DATA_TYPE = 'bytea') OR
    (C.COLUMN_NAME = 'seq' AND C.DATA_TYPE = 'integer')
GROUP BY C.TABLE_SCHEMA, C.TABLE_NAME
HAVING COUNT(*) = 5
";
}