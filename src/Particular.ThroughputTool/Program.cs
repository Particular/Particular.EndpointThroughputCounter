using System.CommandLine;

if (!await Versioning.CheckForCurrentVersion())
{
    return -1;
}

var rootCommand = new RootCommand("A tool to measure NServiceBus endpoints and throughput.");

rootCommand.AddCommand(RabbitMqCommand.CreateCommand());
rootCommand.AddCommand(ServiceControlCommand.CreateCommand());
rootCommand.AddCommand(SqlServerCommand.CreateCommand());
rootCommand.AddCommand(AzureServiceBusCommand.CreateCommand());

return await rootCommand.InvokeAsync(args);
