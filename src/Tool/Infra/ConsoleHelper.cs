using System;
using System.IO;

public static class ConsoleHelper
{
    static object writePadlock = new object();

    public static void WriteError(string message)
    {
        WriteError(writer =>
        {
            writer.Write(message);
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
}
