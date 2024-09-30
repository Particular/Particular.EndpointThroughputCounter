using System.CommandLine;
using System.CommandLine.Parsing;
//using Npgsql;
using Particular.LicensingComponent.Report;
using Particular.ThroughputQuery;
using Particular.ThroughputQuery.PostgreSql;

class PostgreSqlCommand(SharedOptions shared, string[] connectionStrings) : BaseCommand(shared)
{
    static readonly Option<string> ConnectionString = new("--connectionString",
        "A connection string for PostgreSQL that has access to all NServiceBus queue tables");

    static readonly Option<string> ConnectionStringSource = new("--connectionStringSource",
        "A file that contains multiple PostgreSQL connection strings, one connection string per line, for each database that contains NServiceBus queue tables");

    //static readonly Option<string[]> AddCatalogs = new("--addCatalogs")
    //{
    //    Description = "A list of additional database catalogs on the same server containing NServiceBus queue tables",
    //    Arity = ArgumentArity.OneOrMore,
    //    AllowMultipleArgumentsPerToken = true
    //};

    public static Command CreateCommand()
    {
        var command = new Command("postgresql", "Measure endpoints in PostgreSQL transport using the direct query method");

        command.AddOption(ConnectionString);
        command.AddOption(ConnectionStringSource);
        //command.AddOption(AddCatalogs);

        command.SetHandler(async context =>
        {
            var shared = SharedOptions.Parse(context);
            var connectionStrings = GetConnectionStrings(context.ParseResult);
            var cancellationToken = context.GetCancellationToken();

            var runner = new PostgreSqlCommand(shared, connectionStrings);
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
        //var addCatalogs = parsed.GetValueForOption(AddCatalogs);

        if (single is null)
        {
            throw new InvalidOperationException($"No connection strings were provided.");
        }

        return [single];

        //if (addCatalogs is null || !addCatalogs.Any())
        //{
        //    return [single];
        //}

        //var builder = new NpgsqlConnectionStringBuilder
        //{
        //    ConnectionString = single
        //};

        //var dbKey = builder["Initial Catalog"] is not null ? "Initial Catalog" : "Database";

        //var list = new List<string> { single };

        //foreach (var db in addCatalogs)
        //{
        //    builder["Database"] = db;
        //    list.Add(builder.ToString());
        //}

        //return list.ToArray();
    }

    DatabaseDetails[] databases;
    string scopeType;
    Func<QueueTableName, string> getScope;

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

            var tables = databases.SelectMany(db => db.Tables).ToArray();

            //var catalogCount = tables.Select(t => t.DatabaseName).Distinct().Count();
            var schemaCount = tables.Select(t => $"{t.DatabaseName}/{t.Schema}").Distinct().Count();
            var queueNames = tables.Select(t => t.DisplayName).OrderBy(x => x).ToArray();

            //if (catalogCount > 1)
            //{
            //    if (schemaCount > 1)
            //    {
            //        scopeType = "Catalog & Schema";
            //        getScope = t => t.DatabaseNameAndSchema;
            //    }
            //    else
            //    {
            //        scopeType = "Catalog";
            //        getScope = t => t.DatabaseName;
            //    }
            //}

            if (schemaCount > 1)
            {
                scopeType = "Schema";
                getScope = t => t.Schema;
            }
            else
            {
                getScope = t => null;
            }

            return new EnvironmentDetails
            {
                MessageTransport = "PostgreSql",
                ReportMethod = "PostgreSqlQuery",
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