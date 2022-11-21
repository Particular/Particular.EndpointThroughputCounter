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

        Console.WriteLine("Waiting to collect maximum row versions...");
        while (DateTimeOffset.Now < targetEnd)
        {
            var timeLeft = targetEnd - DateTimeOffset.Now;
            Console.Write($"\rWait Time Left: {timeLeft:hh':'mm':'ss} - {sampledQueues}/{totalQueues} sampled");
            await Task.Delay(250, cancellationToken);
        }

        Console.WriteLine();
        Console.WriteLine();

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
        Console.WriteLine("Sampling started for maximum row versions...");
        Console.WriteLine("(This may still take up to 15 minutes depending on queue traffic.)");
        int counter = 0;
        int totalMsWaited = 0;
        sampledQueues = 0;

        DateTimeOffset maxWaitUntil = DateTimeOffset.Now.AddMinutes(15);

        var allTables = databases.SelectMany(db => db.Tables).ToArray();

        while (sampledQueues < totalQueues && DateTimeOffset.Now < maxWaitUntil)
        {
            foreach (var db in databases)
            {
                using (var conn = await db.OpenConnectionAsync(cancellationToken))
                {
                    foreach (var table in db.Tables.Where(t => t.MaxRowVersion is null))
                    {
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = $"select max(RowVersion) from {table.FullName} with (nolock);";
                            var value = await cmd.ExecuteScalarAsync(cancellationToken);

                            if (value is long rowversion)
                            {
                                table.MaxRowVersion = rowversion;
                                // If Min version isn't filled in and we somehow got lucky now, mark it down
                                table.MinRowVersion ??= rowversion;
                            }
                        }
                    }
                }
            }

            sampledQueues = allTables.Count(t => t.MaxRowVersion is not null);
            Console.Write($"\r{sampledQueues}/{totalQueues} sampled");
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

        // Clear out from the Write('\r') pattern
        Console.WriteLine();
        Console.WriteLine();
    }

    async Task GetMinimums(CancellationToken cancellationToken)
    {
        Console.WriteLine("Sampling started for minimum row versions...");
        int counter = 0;
        int totalMsWaited = 0;

        var allTables = databases.SelectMany(db => db.Tables).ToArray();

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
                    using (var conn = await db.OpenConnectionAsync(cancellationToken))
                    {
                        foreach (var table in db.Tables.Where(t => t.MinRowVersion is null))
                        {
                            using (var cmd = conn.CreateCommand())
                            {
                                cmd.CommandText = $"select min(RowVersion) from {table.FullName} with (nolock);";
                                var value = await cmd.ExecuteScalarAsync(cancellationToken);

                                if (value is long rowversion)
                                {
                                    table.MinRowVersion = rowversion;
                                }
                            }
                        }
                    }
                }

                sampledQueues = allTables.Count(t => t.MinRowVersion is not null);
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
            Console.WriteLine();
            Console.WriteLine("Sampling of minimum row versions completed for all tables successfully.");
            Console.WriteLine();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            int successCount = allTables.Count(t => t.MinRowVersion is not null);
            Console.WriteLine($"Sampling of minimum row versions interrupted. Captured {successCount}/{allTables.Length} values.");
        }
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        databases = connectionStrings.Select(connStr => new DatabaseDetails(connStr)).ToArray();

        foreach (var db in databases)
        {
            await db.GetTables(cancellationToken);

            if (!db.Tables.Any())
            {
                throw new HaltException(5, $"ERROR: We were unable to locate any queues in the database '{db.DatabaseName}'. Please check the provided connection string(s) and try again.");
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

    const string GetQueueListCommandText = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

SELECT DISTINCT t.TABLE_SCHEMA AS TableSchema, t.TABLE_NAME as TableName
FROM [INFORMATION_SCHEMA].[TABLES] t WITH (NOLOCK)
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cId WITH (NOLOCK) ON t.TABLE_NAME = cId.TABLE_NAME AND cId.COLUMN_NAME = 'Id' AND cId.DATA_TYPE = 'uniqueidentifier'
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cCorrelationId WITH (NOLOCK) ON t.TABLE_NAME = cCorrelationId.TABLE_NAME AND cCorrelationId.COLUMN_NAME = 'CorrelationId' AND cCorrelationId.DATA_TYPE = 'varchar'
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cReplyToAddress WITH (NOLOCK) ON t.TABLE_NAME = cReplyToAddress.TABLE_NAME AND cReplyToAddress.COLUMN_NAME = 'ReplyToAddress' AND cReplyToAddress.DATA_TYPE = 'varchar'
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cRecoverable WITH (NOLOCK) ON t.TABLE_NAME = cRecoverable.TABLE_NAME AND cRecoverable.COLUMN_NAME = 'Recoverable' AND cRecoverable.DATA_TYPE = 'bit'
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cExpires WITH (NOLOCK) ON t.TABLE_NAME = cExpires.TABLE_NAME AND cExpires.COLUMN_NAME = 'Expires' AND cExpires.DATA_TYPE = 'datetime'
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cHeaders WITH (NOLOCK) ON t.TABLE_NAME = cHeaders.TABLE_NAME AND cHeaders.COLUMN_NAME = 'Headers'
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cBody WITH (NOLOCK) ON t.TABLE_NAME = cBody.TABLE_NAME AND cBody.COLUMN_NAME = 'Body' AND cBody.DATA_TYPE = 'varbinary'
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] cRowVersion WITH (NOLOCK) ON t.TABLE_NAME = cRowVersion.TABLE_NAME AND cRowVersion.COLUMN_NAME = 'RowVersion' AND cRowVersion.DATA_TYPE = 'bigint'
WHERE t.TABLE_TYPE = 'BASE TABLE'";

    class DatabaseDetails
    {
        readonly string connectionString;

        public string DatabaseName { get; }
        public List<TableDetails> Tables { get; private set; }

        public DatabaseDetails(string connectionString)
        {
            this.connectionString = connectionString;

            var builder = new SqlConnectionStringBuilder { ConnectionString = connectionString };
            DatabaseName = (builder["Initial Catalog"] as string) ?? (builder["Database"] as string);
        }

        public int TableCount => Tables.Count;
        public void RemoveIgnored() => Tables.RemoveAll(t => t.IgnoreTable());

        public async Task GetTables(CancellationToken cancellationToken = default)
        {
            List<TableDetails> tables = null;

            using (var conn = await OpenConnectionAsync(cancellationToken))
            {
                tables = (await conn.QueryAsync<TableDetails>(GetQueueListCommandText)).ToList();
            }

            tables.RemoveAll(t => t.IgnoreTable());
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
    }

    [DebuggerDisplay("{FullName}")]
    class TableDetails
    {
        public string TableSchema { get; init; }
        public string TableName { get; init; }
        public long? MinRowVersion { get; set; }
        public long? MaxRowVersion { get; set; }
        public DatabaseDetails Database { get; set; }

        public string FullName => $"[{TableSchema}].[{TableName}]";

        public string DisplayName => Database is null ? FullName : $"[{Database.DatabaseName}].{FullName}";

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
            if (MinRowVersion is not null && MaxRowVersion is not null)
            {
                var throughput = (int)(MaxRowVersion.Value - MinRowVersion.Value);
                return new QueueThroughput { QueueName = DisplayName, Throughput = throughput };
            }

            // For now, not being able to detect messages probably means the true value
            // is close enough to zero that it doesn't matter.
            return new QueueThroughput { QueueName = DisplayName, Throughput = 0 };
        }
    }

}