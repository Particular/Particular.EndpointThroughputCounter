using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Data.SqlClient;
using Particular.LicensingComponent.Report;
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
            var cancellationToken = context.GetCancellationToken();

            try
            {
                var connectionStrings = GetConnectionStrings(context.ParseResult);

                var runner = new SqlServerCommand(shared, connectionStrings);
                await runner.Run(cancellationToken);
            }
            catch (HaltException halt)
            {
                Out.WriteLine();
                Out.WriteError(halt.Message);
                Environment.ExitCode = halt.ExitCode;
            }
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

            return ParseConnectionStringSource(File.ReadAllLines(sourcePath), sourcePath);
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

    internal static string[] ParseConnectionStringSource(string[] lines, string sourcePath)
    {
        var connectionStrings = new List<string>();

        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].Trim();

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            if (line.Length >= 2 && ((line[0] == '"' && line[^1] == '"') || (line[0] == '\'' && line[^1] == '\'')))
            {
                line = line[1..^1].Trim();
            }

            try
            {
                _ = new SqlConnectionStringBuilder(line);
            }
            catch (Exception x) when (x is FormatException or ArgumentException or KeyNotFoundException)
            {
                throw new HaltException(HaltReason.InvalidConfig, $"ERROR: Line {i + 1} of '{sourcePath}' could not be parsed as a SQL Server connection string: {x.Message}");
            }

            connectionStrings.Add(line);
        }

        if (connectionStrings.Count == 0)
        {
            throw new HaltException(HaltReason.InvalidConfig, $"ERROR: The file '{sourcePath}' does not contain any connection strings.");
        }

        return connectionStrings.ToArray();
    }

    readonly string[] connectionStrings;
    DatabaseDetails[] databases;
    string scopeType;
    Func<QueueTableName, string> getScope;

    public SqlServerCommand(SharedOptions shared, string[] connectionStrings)
        : base(shared)
    {
        this.connectionStrings = connectionStrings;
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        try
        {
            databases = connectionStrings.Select(connStr => new DatabaseDetails(connStr)).ToArray();

            foreach (var db in databases)
            {
                Out.WriteLine($"Testing connection to server '{db.DataSource}', database '{db.DatabaseName ?? "(default)"}', Integrated Security={(db.IntegratedSecurity ? "true" : "false")}...");
                if (db.IntegratedSecurity && OperatingSystem.IsWindows())
                {
                    using var identity = System.Security.Principal.WindowsIdentity.GetCurrent();
                    Out.WriteLine($" - Connecting as Windows identity '{identity.Name}'");
                }

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

            var tables = databases.SelectMany(db => db.Tables).ToArray();

            var catalogCount = tables.Select(t => t.DatabaseName).Distinct().Count();
            var schemaCount = tables.Select(t => $"{t.DatabaseName}/{t.Schema}").Distinct().Count();

            if (catalogCount > 1)
            {
                if (schemaCount > 1)
                {
                    scopeType = "Catalog & Schema";
                    getScope = t => t.DatabaseNameAndSchema;
                }
                else
                {
                    scopeType = "Catalog";
                    getScope = t => t.DatabaseName;
                }
            }
            else if (schemaCount > 1)
            {
                scopeType = "Schema";
                getScope = t => t.Schema;
            }
            else
            {
                getScope = t => null;
            }
            var queueNames = tables.Select(t => new ScopeAndQueue(getScope(t), t.DisplayName)).OrderBy(x => x.QueueName).ThenBy(x => x.Scope).ToArray();

            return new EnvironmentDetails
            {
                MessageTransport = "SqlTransport",
                ReportMethod = "SqlServerQuery",
                ScopeType = scopeType,
                QueueNames = queueNames
            };
        }
        catch (QueryException x)
        {
            throw new HaltException(x);
        }
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var start = DateTimeOffset.Now;
        var targetEnd = start.UtcDateTime + PollingRunTime;

        Out.WriteLine("Sampling queue table initial values...");
        var startDbTasks = databases.Select(db => db.GetSnapshot(cancellationToken)).ToArray();
        var startData = await Task.WhenAll(startDbTasks);

        Out.WriteLine("Waiting to collect final values...");
        await Out.CountdownTimer("Wait Time Left", targetEnd, cancellationToken: cancellationToken);

        Out.WriteLine("Sampling queue table final values...");
        var endDbTasks = databases.Select(db => db.GetSnapshot(cancellationToken)).ToArray();
        var endData = await Task.WhenAll(endDbTasks);

        var end = DateTimeOffset.Now;

        var allStart = startData.SelectMany(db => db);
        var allEnd = endData.SelectMany(db => db);
        var queues = DatabaseDetails.CalculateThroughput(allStart, allEnd)
            .Select(t => new QueueThroughput
            {
                QueueName = t.Name,
                Throughput = t.Throughput,
                Scope = getScope(t)
            })
            .OrderBy(q => q.QueueName)
            .ToArray();

        return new QueueDetails
        {
            ScopeType = scopeType,
            StartTime = start,
            EndTime = end,
            Queues = queues
        };
    }



}