using System.CommandLine;

#if !WINX64
if (!await Versioning.CheckForCurrentVersion())
{
    return -1;
}
#endif

var rootCommand = new RootCommand("A tool to measure NServiceBus endpoints and throughput.");

rootCommand.AddCommand(RabbitMqCommand.CreateCommand());
rootCommand.AddCommand(ServiceControlCommand.CreateCommand());
rootCommand.AddCommand(SqlServerCommand.CreateCommand());
rootCommand.AddCommand(AzureServiceBusCommand.CreateCommand());
rootCommand.AddCommand(SqsCommand.CreateCommand());

return await rootCommand.InvokeAsync(args);
