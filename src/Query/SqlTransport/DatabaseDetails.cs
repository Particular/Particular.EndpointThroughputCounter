﻿namespace Particular.ThroughputQuery.SqlTransport
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

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
                var builder = new SqlConnectionStringBuilder { ConnectionString = connectionString, TrustServerCertificate = true };
                DatabaseName = builder["Initial Catalog"] as string ?? builder["Database"] as string;
                this.connectionString = builder.ToString();
            }
            catch (Exception x) when (x is FormatException or ArgumentException)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, "There's something wrong with the SQL connection string and it could not be parsed.", x);
            }
        }

        public int TableCount => Tables.Count;

        public async Task TestConnection(CancellationToken cancellationToken = default)
        {
            try
            {
                await TestGetServerName(cancellationToken).ConfigureAwait(false);
            }
            catch (SqlException x) when (IsConnectionOrLoginIssue(x))
            {
                throw new QueryException(QueryFailureReason.Auth, "Could not access SQL database. Is the connection string correct?", x);
            }
        }

        static bool IsConnectionOrLoginIssue(SqlException x)
        {
            // Reference is here: https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/cc645603(v=sql.105)?redirectedfrom=MSDN

            return x.Number switch
            {
                // Unproven or negative codes that need further "proof" here. If we get a false negative because of a localized exception message, so be it.
                // -2: Microsoft.Data.SqlClient.SqlException (0x80131904): Connection Timeout Expired.  The timeout period elapsed while attempting to consume the pre-login handshake acknowledgement.  This could be because the pre-login handshake failed or the server was unable to respond back in time.  The duration spent while attempting to connect to this server was - [Pre-Login] initialization=21041; handshake=4;
                -2 => x.Message.Contains("Connection Timeout Expired"),
                0 => x.Message.Contains("Failed to authenticate") || x.Message.Contains("server was not found"),


                // 10060: A network-related or instance-specific error occurred while establishing a connection to SQL Server. The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server is configured to allow remote connections.
                // 10061: A network-related or instance-specific error occurred while establishing a connection to SQL Server. The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server is configured to allow remote connections. (provider: TCP Provider, error: 0 - No connection could be made because the target machine actively refused it.
                10060 or 10061 => true,

                // 233: Named pipes: No process is on the other end of the pipe
                // 53: A network-related or instance-specific error occurred while establishing a connection to SQL Server
                // -2146893022: The target principal name is incorrect
                // 4060: Cannot open database "%.*ls" requested by the login. The login failed.
                // 4064: Cannot open user default database. Login failed.
                // 9758: Login protocol negotiation error occurred.
                // 14520: %s is not a valid SQL Server standard login, Windows NT user, Windows NT group, or msdb database role.
                // 15007: '%s' is not a valid login or you do not have permission.
                // 15537: Login '%.*ls' does not have access to server.
                // 15538: Login '%.*ls' does not have access to database.
                // 17197: Login failed due to timeout; the connection has been closed. This error may indicate heavy server load. Reduce the load on the server and retry login.%.*ls
                // 17892: Logon failed for login '%.*ls' due to trigger execution.%.*ls
                233 or 53 or -2146893022 or 4060 or 4064 or 9758 or 14520 or 15007 or 15537 or 15538 or 17197 or 17892 => true,

                // Pretty much every error in this range is login-related
                // https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/cc645934(v=sql.105)
                >= 18301 and <= 18496 => true,

                // 21142: The SQL Server '%s' could not obtain Windows group membership information for login '%s'. Verify that the Windows account has access to the domain of the login.
                // 28034: Connection handshake failed.The login '%.*ls' does not have CONNECT permission on the endpoint.State % d.
                // 33041: Cannot create login token for existing authenticators. If dbo is a windows user make sure that its windows account information is accessible to SQL Server.
                21142 or 28034 or 33041 => true,

                // Everything else
                _ => false
            };
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
            List<QueueTableName> tables = [];

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

        public async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            var conn = new SqlConnection(connectionString);
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

            if (tableName is "ServiceControl.ThroughputData")
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Query works by finding all the columns in any table that *could* be from an NServiceBus
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