using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Particular.EndpointThroughputCounter.Data;

class SqlServerCommand : BaseCommand
{
    static readonly Option<string> ConnectionString = new("--connectionString",
        "A connection string for SQL Server that has access to all NServiceBus queue tables");

    static readonly Option<string> ConnectionStringSource = new("--connectionStringSource",
        "A file that contains multiple SQL Server connection strings, one connection string per line, for each database catalog that contains NServiceBus queue tables");

    static readonly Option<string[]> AddCatalogs = new("--addCatalogs")
    {
        Description = "A list of additional database catalogs on the same server containing NServiceBus queue tables",
        Arity = ArgumentArity.OneOrMore,
        AllowMultipleArgumentsPerToken = true
    };

    public static Command CreateCommand()
    {
        var command = new Command("sqlserver", "Measure endpoints in SQL Server transport using the direct query method");

        command.AddOption(ConnectionString);
        command.AddOption(ConnectionStringSource);
        command.AddOption(AddCatalogs);

        command.SetHandler(async context =>
        {
            var shared = SharedOptions.Parse(context);
            var connectionStrings = GetConnectionStrings(context.ParseResult);
            var cancellationToken = context.GetCancellationToken();

            var runner = new SqlServerCommand(shared, connectionStrings);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    static string[] GetConnectionStrings(ParseResult parsed)
    {
        var sourcePath = parsed.GetValueForOption(ConnectionStringSource);

        if (!string.IsNullOrEmpty(sourcePath))
        {
            if (!Path.IsPathFullyQualified(sourcePath))
            {
                sourcePath = Path.GetFullPath(Path.Join(Environment.CurrentDirectory, sourcePath));
            }
            if (!File.Exists(sourcePath))
            {
                throw new FileNotFoundException($"Could not find file specified by {ConnectionStringSource.Name} parameter", sourcePath);
            }

            return File.ReadAllLines(sourcePath)
                .Where(line => !string.IsNullOrWhiteSpace(line))
                .ToArray();
        }

        var single = parsed.GetValueForOption(ConnectionString);
        var addCatalogs = parsed.GetValueForOption(AddCatalogs);

        if (single is null)
        {
            throw new InvalidOperationException($"No connection strings were provided.");
        }

        if (addCatalogs is null || !addCatalogs.Any())
        {
            return new[] { single };
        }

        var builder = new SqlConnectionStringBuilder
        {
            ConnectionString = single
        };

        var dbKey = builder["Initial Catalog"] is not null ? "Initial Catalog" : "Database";

        var list = new List<string> { single };

        foreach (var db in addCatalogs)
        {
            builder[dbKey] = db;
            list.Add(builder.ToString());
        }

        return list.ToArray();
    }

    readonly string[] connectionStrings;
    int totalQueues;
    int sampledQueues;
    DatabaseDetails[] databases;

    public SqlServerCommand(SharedOptions shared, string[] connectionStrings)
        : base(shared)
    {
        this.connectionStrings = connectionStrings;
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        foreach (var db in databases)
        {
            db.RemoveIgnored();
        }
        totalQueues = databases.Sum(d => d.TableCount);
        sampledQueues = 0;

        var start = DateTimeOffset.Now;
        var targetEnd = start + PollingRunTime;

        var tokenSource = new CancellationTokenSource();
        var getMinimumsTask = GetMinimums(tokenSource.Token);

        Out.WriteLine("Waiting to collect maximum row versions...");
        while (DateTimeOffset.Now < targetEnd)
        {
            var timeLeft = targetEnd - DateTimeOffset.Now;
            Out.Progress($"Wait Time Left: {timeLeft:hh':'mm':'ss} - {sampledQueues}/{totalQueues} sampled");
            await Task.Delay(250, cancellationToken);
        }

        Out.EndProgress();
        Out.WriteLine();

        tokenSource.Cancel();
        await getMinimumsTask;

        await GetMaximums(cancellationToken);

        var end = DateTimeOffset.Now;
        var queues = databases.SelectMany(db => db.Tables).Select(t => t.ToQueueThroughput()).OrderBy(q => q.QueueName).ToArray();

        return new QueueDetails
        {
            StartTime = start,
            EndTime = end,
            Queues = queues
        };
    }

    async Task GetMaximums(CancellationToken cancellationToken)
    {
        Out.WriteLine("Sampling started for maximum row versions...");
        Out.WriteLine("(This may still take up to 15 minutes depending on queue traffic.)");
        int counter = 0;
        int totalMsWaited = 0;
        sampledQueues = 0;

        DateTimeOffset maxWaitUntil = DateTimeOffset.Now.AddMinutes(15);

        var allTables = databases.SelectMany(db => db.Tables).ToArray();
        var sqlExceptions = new Queue<SqlException>(5);

        foreach (var db in databases)
        {
            db.LogSuccessOrFailure(true, reset: true);
        }

        while (sampledQueues < totalQueues && DateTimeOffset.Now < maxWaitUntil)
        {
            foreach (var db in databases)
            {
                try
                {
                    using (var conn = await db.OpenConnectionAsync(cancellationToken))
                    {
                        foreach (var table in db.Tables.Where(t => t.EndRowVersion is null))
                        {
                            var rowversion = await table.GetMaxRowVersion(conn, cancellationToken);
                            if (rowversion != null)
                            {
                                table.EndRowVersion = rowversion;
                                // If start value isn't filled in and we somehow got lucky now, mark it down, though this will result in 0 throughput anyway
                                table.StartRowVersion ??= rowversion;
                            }
                        }
                    }
                    db.LogSuccessOrFailure(true);
                }
                catch (SqlException x)
                {
                    db.LogSuccessOrFailure(false);
                    sqlExceptions.Enqueue(x);
                    while (sqlExceptions.Count > 5)
                    {
                        _ = sqlExceptions.Dequeue();
                    }
                }
            }

            sampledQueues = allTables.Count(t => t.EndRowVersion is not null);
            Out.Progress($"{sampledQueues}/{totalQueues} sampled");
            if (sampledQueues < totalQueues)
            {
                await Task.Delay(GetDelayMilliseconds(ref counter, ref totalMsWaited), cancellationToken);

                // After every 5 minutes of delay (if necessary) reset to the fast query pattern
                // This could be 3 rounds during the 15 minutes
                if (totalMsWaited > 5 * 60 * 1000)
                {
                    counter = 0;
                    totalMsWaited = 0;
                }
            }
        }

        Out.EndProgress();
        Out.WriteLine();

        if (databases.Any(db => db.Tables.Any(t => t.EndRowVersion is null) && db.ErrorCount > 0))
        {
            throw new AggregateException("Unable to sample maximum row versions without repeated SqlExceptions. The last 5 exception instances are included.", sqlExceptions);
        }
    }

    async Task GetMinimums(CancellationToken cancellationToken)
    {
        Out.WriteLine("Sampling started for minimum row versions...");
        int counter = 0;
        int totalMsWaited = 0;

        var allTables = databases.SelectMany(db => db.Tables).ToArray();
        var sqlExceptions = new Queue<SqlException>();

        try
        {
            while (sampledQueues < totalQueues)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                foreach (var db in databases)
                {
                    try
                    {
                        using (var conn = await db.OpenConnectionAsync(cancellationToken))
                        {
                            foreach (var table in db.Tables.Where(t => t.StartRowVersion is null))
                            {
                                table.StartRowVersion = await table.GetMaxRowVersion(conn, cancellationToken);
                            }
                        }
                        db.LogSuccessOrFailure(true);
                    }
                    catch (SqlException x)
                    {
                        db.LogSuccessOrFailure(false);
                        sqlExceptions.Enqueue(x);
                        while (sqlExceptions.Count > 5)
                        {
                            _ = sqlExceptions.Dequeue();
                        }
                    }
                }

                sampledQueues = allTables.Count(t => t.StartRowVersion is not null);
                if (sampledQueues < totalQueues)
                {
                    await Task.Delay(GetDelayMilliseconds(ref counter, ref totalMsWaited), cancellationToken);

                    // After every 15 minutes of delay (if necessary) reset to the fast query pattern
                    if (totalMsWaited > 15 * 60 * 1000)
                    {
                        counter = 0;
                        totalMsWaited = 0;
                    }
                }
            }

            Out.WriteLine();
            Out.WriteLine("Sampling of minimum row versions completed for all tables successfully.");
            Out.WriteLine();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            int successCount = allTables.Count(t => t.StartRowVersion is not null);
            Out.WriteLine($"Sampling of minimum row versions interrupted. Captured {successCount}/{allTables.Length} values.");
        }
        finally
        {
            if (databases.Any(db => db.Tables.Any(t => t.StartRowVersion is null) && db.ErrorCount > 0))
            {
                throw new AggregateException("Unable to sample minimum row versions without repeated SqlExceptions. The last 5 exception instances are included.", sqlExceptions);
            }
        }
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        databases = connectionStrings.Select(connStr => new DatabaseDetails(connStr)).ToArray();

        foreach (var db in databases)
        {
            await db.TestConnection(cancellationToken);
        }

        foreach (var db in databases)
        {
            await db.GetTables(cancellationToken);

            if (!db.Tables.Any())
            {
                throw new HaltException(HaltReason.InvalidEnvironment, $"ERROR: We were unable to locate any queues in the database '{db.DatabaseName}'. Please check the provided connection string(s) and try again.");
            }
        }

        var queueNames = databases.SelectMany(db => db.Tables).Select(t => t.DisplayName).OrderBy(x => x).ToArray();

        return new EnvironmentDetails
        {
            MessageTransport = "SqlTransport",
            ReportMethod = "SqlServerQuery",
            QueueNames = queueNames
        };
    }

    static int GetDelayMilliseconds(ref int counter, ref int totalMsWaited)
    {
        var waitMs = counter switch
        {
            < 20 => 50,     // 20 times in the first second
            < 100 => 100,   // 10 times/sec in the next 8 seconds
            < 160 => 1000,  // Once per second for a minute
            _ => 10_000,     // Once every 10s after that
        };

        counter++;
        totalMsWaited += waitMs;

        return waitMs;
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

    class DatabaseDetails
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
                throw new HaltException(HaltReason.InvalidEnvironment, "There's something wrong with the SQL connection string and it could not be parsed.", x);
            }
        }

        public int TableCount => Tables.Count;
        public void RemoveIgnored() => Tables.RemoveAll(t => t.IgnoreTable());

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
                using (var conn = await OpenConnectionAsync(cancellationToken))
                {
                    _ = await conn.ExecuteScalarAsync<string>("select @@SERVERNAME");
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
                using (var conn = await OpenConnectionAsync(cancellationToken))
                {
                    _ = await conn.ExecuteScalarAsync<string>("select @@SERVERNAME");
                }
            }
            catch (SqlException x) when (x.Number is 233 or 18456 or 53)
            {
                throw new HaltException(HaltReason.Auth, "Could not access SQL database. Is the connection string correct?", x);
            }
        }

        public async Task GetTables(CancellationToken cancellationToken = default)
        {
            List<TableDetails> tables = null;

            using (var conn = await OpenConnectionAsync(cancellationToken))
            {
                tables = (await conn.QueryAsync<TableDetails>(GetQueueListCommandText)).ToList();
            }

            _ = tables.RemoveAll(t => t.IgnoreTable());
            foreach (var table in tables)
            {
                table.Database = this;
            }

            Tables = tables;
        }

        public async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
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
    }

    [DebuggerDisplay("{FullName}")]
    class TableDetails
    {
        public string TableSchema { get; init; }
        public string TableName { get; init; }
        public long? StartRowVersion { get; set; }
        public long? EndRowVersion { get; set; }
        public DatabaseDetails Database { get; set; }

        public string FullName => $"[{TableSchema}].[{TableName}]";

        public string DisplayName => Database is null ? FullName : $"[{Database.DatabaseName}].{FullName}";

        public async Task<long?> GetMaxRowVersion(SqlConnection conn, CancellationToken cancellationToken = default)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"select max(RowVersion) from {FullName} with (nolock);";
                var value = await cmd.ExecuteScalarAsync(cancellationToken);

                if (value is long rowversion)
                {
                    return rowversion;
                }
            }

            return null;
        }

        public bool IgnoreTable()
        {
            if (TableName is "error" or "audit")
            {
                return true;
            }

            if (TableName.StartsWith("Particular.", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }

        public QueueThroughput ToQueueThroughput()
        {
            if (StartRowVersion is not null && EndRowVersion is not null)
            {
                var throughput = (int)(EndRowVersion.Value - StartRowVersion.Value);
                return new QueueThroughput { QueueName = DisplayName, Throughput = throughput };
            }

            // For now, not being able to detect messages probably means the true value
            // is close enough to zero that it doesn't matter.
            return new QueueThroughput { QueueName = DisplayName, Throughput = 0 };
        }
    }

}