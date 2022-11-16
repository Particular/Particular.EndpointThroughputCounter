﻿using System;
using System.CommandLine;

if (!await Versioning.CheckForCurrentVersion())
{
    return -1;
}

ConsoleHelper.SetupUnhandledExceptionHandling();

try
{
    var rootCommand = new RootCommand("A tool to measure NServiceBus endpoints and throughput.");

    rootCommand.AddCommand(RabbitMqCommand.CreateCommand());
    rootCommand.AddCommand(ServiceControlCommand.CreateCommand());
    rootCommand.AddCommand(SqlServerCommand.CreateCommand());
    rootCommand.AddCommand(AzureServiceBusCommand.CreateCommand());
    rootCommand.AddCommand(SqsCommand.CreateCommand());

    rootCommand.AddGlobalOption(SharedOptions.MaskNames);

    var returnCode = await rootCommand.InvokeAsync(args);
    return returnCode;
}
catch (Exception x)
{
    ConsoleHelper.WriteError(w =>
    {
        w.WriteLine(x);
        w.WriteLine();
        w.WriteLine("Unable to execute command, please contact Particular Software support.");
    });

    return -1;
}