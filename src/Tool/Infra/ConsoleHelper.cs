using System;
using System.IO;

public static class ConsoleHelper
{
    static object writePadlock = new object();

    public static void WriteError(string message)
    {
        WriteError(writer =>
        {
            writer.WriteLine(message);
        });
    }

    public static void WriteError(Action<TextWriter> writeToError)
    {
        lock (writePadlock)
        {
            var current = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;

            writeToError(Console.Error);

            Console.ForegroundColor = current;
        }
    }

    public static void SetupUnhandledExceptionHandling()
    {
        AppDomain.CurrentDomain.UnhandledException += (sender, e) =>
        {
            WriteError(w =>
            {
                // Just to tell the difference between e.Terminating or not based on the log
                var msg = $"An unhandled exception was caught{(e.IsTerminating ? ", forcing a runtime exit" : "")}.";

                Console.WriteLine();
                Console.WriteLine(msg);
                Console.WriteLine(e.ExceptionObject);
                Console.WriteLine();
                Console.WriteLine("Contact Particular Software support for assistance.");

                Environment.Exit(-3);
            });
        };
    }
}
