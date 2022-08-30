using System.CommandLine;
using System.Diagnostics;
using Dapper;
using Particular.ThroughputTool.Data;
using Microsoft.Data.SqlClient;

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

    public SqlServerCommand(string outputPath, string[] maskNames, string connectionString)
        : base(outputPath, maskNames)
    {
        this.connectionString = connectionString;
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var tables = await GetTables(cancellationToken);

        tables.RemoveAll(t => t.IgnoreTable());

        var start = DateTimeOffset.Now;

        var targetEnd = start.AddMinutes(1);

        await SampleAll(tables, cancellationToken);
        while (DateTimeOffset.Now < targetEnd)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            await SampleAll(tables, cancellationToken);
        }

        var end = DateTimeOffset.Now;

        return new QueueDetails
        {
            StartTime = start,
            EndTime = end,
            Queues = tables.Select(t => t.ToQueueThroughput()).OrderBy(q => q.QueueName).ToArray()
        };
    }

    async Task SampleAll(List<TableDetails> tables, CancellationToken cancellationToken)
    {
        using (var conn = await OpenConnectionAsync(cancellationToken))
        {
            foreach (var table in tables)
            {
                await table.Sample(conn, cancellationToken);
            }
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
        public long? MinRowVersion { get; private set; }
        public long? MaxRowVersion { get; private set; }
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

        public async Task Sample(SqlConnection conn, CancellationToken cancellationToken = default)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"select max(RowVersion) from [{TableSchema}].[{TableName}] with (nolock);";
                var value = await cmd.ExecuteScalarAsync(cancellationToken);

                if (value is not DBNull)
                {
                    var rowversion = (long)value;
                    if (MaxRowVersion is null || rowversion > MaxRowVersion)
                    {
                        MaxRowVersion = rowversion;
                    }
                    if (MinRowVersion is null || rowversion < MinRowVersion)
                    {
                        MinRowVersion = rowversion;
                    }
                }
            }
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