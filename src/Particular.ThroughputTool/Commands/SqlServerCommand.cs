using System.CommandLine;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Dapper;
using Microsoft.Data.SqlClient;
using Particular.ThroughputTool.Data;

class SqlServerCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("sqlserver", "Measure endpoints in SQL Server transport using the direct query method");

        var connStrArg = new Option<string>(
            name: "--connectionString",
            description: "A connection string for SQL Server that has access to all queue tables");

        command.AddOption(connStrArg);

        var maskNames = SharedOptions.CreateMaskNamesOption();
        var outputPath = SharedOptions.CreateOutputPathOption();
        command.AddOption(maskNames);
        command.AddOption(outputPath);

        command.SetHandler(async (connStr, maskNames, outputPath) =>
        {
            var runner = new SqlServerCommand(outputPath, maskNames, connStr);
            await runner.Run(CancellationToken.None);
        },
        connStrArg, maskNames, outputPath);

        return command;
    }

    readonly string connectionString;
    int totalQueues;
    int sampledQueues;

    public SqlServerCommand(string outputPath, string[] maskNames, string connectionString)
        : base(outputPath, maskNames)
    {
        this.connectionString = connectionString;
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var tables = await GetTables(cancellationToken);

        tables.RemoveAll(t => t.IgnoreTable());
        totalQueues = tables.Count;
        sampledQueues = 0;

        var start = DateTimeOffset.Now;
        var targetEnd = start + PollingRunTime;

        var tokenSource = new CancellationTokenSource();
        var getMinimumsTask = GetMinimums(tables, tokenSource.Token);

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

        await GetMaximums(tables, cancellationToken);

        var end = DateTimeOffset.Now;

        return new QueueDetails
        {
            StartTime = start,
            EndTime = end,
            Queues = tables.Select(t => t.ToQueueThroughput()).OrderBy(q => q.QueueName).ToArray()
        };
    }

    async Task GetMaximums(List<TableDetails> tables, CancellationToken cancellationToken)
    {
        Console.WriteLine("Sampling started for maximum row versions...");
        Console.WriteLine("(This may still take up to 15 minutes depending on queue traffic.)");
        int counter = 0;
        int totalMsWaited = 0;
        sampledQueues = 0;

        DateTimeOffset maxWaitUntil = DateTimeOffset.Now.AddMinutes(15);

        while (sampledQueues < totalQueues && DateTimeOffset.Now < maxWaitUntil)
        {
            using (var conn = await OpenConnectionAsync(cancellationToken))
            {
                foreach (var table in tables.Where(t => t.MaxRowVersion is null))
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

            sampledQueues = tables.Count(t => t.MaxRowVersion is not null);
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

    async Task GetMinimums(List<TableDetails> tables, CancellationToken cancellationToken)
    {
        Console.WriteLine("Sampling started for minimum row versions...");
        int counter = 0;
        int totalMsWaited = 0;

        try
        {
            while (sampledQueues < totalQueues)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                using (var conn = await OpenConnectionAsync(cancellationToken))
                {
                    foreach (var table in tables.Where(t => t.MinRowVersion is null))
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

                sampledQueues = tables.Count(t => t.MinRowVersion is not null);
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
            int successCount = tables.Count(t => t.MinRowVersion is not null);
            Console.WriteLine($"Sampling of minimum row versions interrupted. Captured {successCount}/{tables.Count} values.");
        }
    }

    async Task<List<TableDetails>> GetTables(CancellationToken cancellationToken)
    {
        using (var conn = await OpenConnectionAsync(cancellationToken))
        {
            return (await conn.QueryAsync<TableDetails>(GetQueueListCommandText)).ToList();
        }
    }

    protected override Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
        => Task.FromResult(new EnvironmentDetails
        {
            MessageTransport = "SqlTransport",
            ReportMethod = "SqlServerQuery"
        });


    async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);
        return conn;
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

    [DebuggerDisplay("{FullName}")]
    class TableDetails
    {
        public string TableSchema { get; init; }
        public string TableName { get; init; }
        public long? MinRowVersion { get; set; }
        public long? MaxRowVersion { get; set; }
        public string FullName => $"[{TableSchema}].[{TableName}]";

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
                return new QueueThroughput { QueueName = FullName, Throughput = throughput };
            }

            return new QueueThroughput { QueueName = FullName, Throughput = -1 };
        }
    }

}