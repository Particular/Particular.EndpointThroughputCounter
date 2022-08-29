﻿using System.CommandLine;

var rootCommand = new RootCommand("A tool to measure NServiceBus endpoints and throughput.");

rootCommand.AddCommand(RabbitMqCommand.CreateCommand());
rootCommand.AddCommand(ServiceControlCommand.CreateCommand());
rootCommand.AddCommand(SqlServerCommand.CreateCommand());

return await rootCommand.InvokeAsync(args);
