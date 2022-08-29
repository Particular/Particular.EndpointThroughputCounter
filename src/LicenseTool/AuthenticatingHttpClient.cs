﻿using System.Net;

class AuthenticatingHttpClient : IDisposable
{
    HttpClient http;
    string currentUser;

    public AuthenticatingHttpClient()
    {
        http = new HttpClient();
    }

    public Task<Stream> GetStreamAsync(string url, CancellationToken cancellationToken = default)
    {
        return RetryLoopOnUnauthorized(url, 3, token => http.GetStreamAsync(url, token), cancellationToken);
    }

    Task<TResult> RetryLoopOnUnauthorized<TResult>(string url, int tries, Func<CancellationToken, Task<TResult>> getResult, CancellationToken cancellationToken)
        => RetryLoopOnUnauthorized(new Uri(url), tries, getResult, cancellationToken);

    async Task<TResult> RetryLoopOnUnauthorized<TResult>(Uri uri, int tries, Func<CancellationToken, Task<TResult>> getResult, CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                return await getResult(cancellationToken);
            }
            catch (HttpRequestException x) when (x.StatusCode == HttpStatusCode.Unauthorized)
            {
                if (--tries <= 0)
                {
                    throw;
                }

                var uriPartial = uri.GetLeftPart(UriPartial.Authority);

                Console.WriteLine($"Unable to access {uriPartial} as {currentUser ?? "default credentials"}.");
                Console.WriteLine();

                Console.WriteLine($"Enter authentication for {uriPartial}:");
                Console.Write("Username: ");
                var user = Console.ReadLine();
                Console.Write("Password: ");
                var pass = ReadPassword();

                var newHandler = new HttpClientHandler
                {
                    Credentials = new NetworkCredential(user, pass)
                };

                var newHttp = new HttpClient(newHandler);
                var oldHttp = http;
                http = newHttp;
                currentUser = user;

                oldHttp.Dispose();
            }
        }
    }

    string ReadPassword()
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
        return pass;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected void Dispose(bool isDisposing)
    {
        if (isDisposing)
        {
            http.Dispose();
        }
    }
}