﻿using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

public static class Out
{
    static readonly StringBuilder output = new StringBuilder();
    static readonly object writePadlock = new object();
    static string lastProgressMessage;

    public static void WriteLine()
    {
        output.AppendLine();
        Console.WriteLine();
    }

    public static void WriteLine(string line)
    {
        Console.WriteLine(line);
        output.AppendLine(line);
    }

    public static void WriteLine(string format, params object[] args)
    {
        Console.WriteLine(format, args);
        output.AppendFormat(format, args).AppendLine();
    }

    public static void Write(string text)
    {
        Console.Write(text);
        output.Append(text);
    }

    public static string ReadLine()
    {
        var result = Console.ReadLine();
        output.AppendLine(result);
        return result;
    }

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

            using (var writer = new StringWriter())
            {
                writeToError(writer);
                var written = writer.ToString();

                Console.Error.Write(written);
                output.Append(written);
            }

            Console.ForegroundColor = current;
        }
    }

    public static string ReadPassword()
    {
        var pass = string.Empty;
        ConsoleKey key;
        do
        {
            var keyInfo = Console.ReadKey(intercept: true);
            key = keyInfo.Key;

            if (key == ConsoleKey.Backspace && pass.Length > 0)
            {
                Console.Write("\b \b");
                pass = pass[0..^1];
            }
            else if (!char.IsControl(keyInfo.KeyChar))
            {
                Console.Write("*");
                pass += keyInfo.KeyChar;
            }
        } while (key != ConsoleKey.Enter);

        Console.WriteLine();
        output.AppendLine(new string('*', pass.Length));
        return pass;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0010:Add missing cases", Justification = "Don't need every key")]
    public static bool Confirm(string prompt)
    {
        Write(prompt);
        Write(" (Y/N): ");
        while (true)
        {
            var key = Console.ReadKey(true);
            switch (key.Key)
            {
                case ConsoleKey.Y:
                    WriteLine("Yes");
                    return true;
                case ConsoleKey.N:
                    WriteLine("No");
                    return false;
                default:
                    continue;
            }
        }
    }

    public static async Task CountdownTimer(string label, DateTime waitUntilUtc, int loopDelayMs = 250, Action onLoopAction = null, CancellationToken cancellationToken = default)
    {
        output.AppendLine($"{label}: <CountdownTimer>");

        while (DateTime.UtcNow < waitUntilUtc)
        {
            onLoopAction?.Invoke();

            var timeLeft = waitUntilUtc - DateTime.UtcNow;
            Console.Write($"\r{label}: {timeLeft:hh':'mm':'ss}");
            await Task.Delay(loopDelayMs, cancellationToken);
        }
        Console.WriteLine();
    }

    public static void Progress(string progressMessage)
    {
        lastProgressMessage = progressMessage;
        Console.Write("\r" + progressMessage);
    }

    public static void EndProgress()
    {
        output.AppendLine(lastProgressMessage);
        Console.WriteLine();
        lastProgressMessage = null;
    }

    public static string GetToolOutput()
    {
        return output.ToString();
    }
}