using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Data.SqlClient;
using Particular.EndpointThroughputCounter.Data;
using Particular.ThroughputQuery;
using Particular.ThroughputQuery.SqlTransport;

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
        var queues = databases.SelectMany(db => db.Tables)
            .Select(t => new QueueThroughput
            {
                QueueName = t.DisplayName,
                Throughput = t.GetThroughput()
            })
            .OrderBy(q => q.QueueName)
            .ToArray();

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
        try
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
        catch (QueryException x)
        {
            throw new HaltException(x);
        }
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

}