using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

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
        return RetryLoopOnUnauthorized(url, 3, token => HttpGetStreamWithUsefulException(url, token), cancellationToken);
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
            catch (HttpRequestExceptionWithStatus x) when (x.StatusCode == HttpStatusCode.Unauthorized)
            {
                Console.WriteLine(x.Exception);

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

    // Replace with http.GetStreamAsync(url) when supporting only net6.0 and greater
    async Task<Stream> HttpGetStreamWithUsefulException(string url, CancellationToken cancellationToken)
    {
        var response = await http.GetAsync(url, cancellationToken);

        try
        {
            response.EnsureSuccessStatusCode();
            var content = response.Content;
#if NET6_0_OR_GREATER
            return await content.ReadAsStreamAsync(cancellationToken);
#else
            return await content.ReadAsStreamAsync();
#endif
        }
        catch (HttpRequestException x)
        {
            throw new HttpRequestExceptionWithStatus(x, response.StatusCode);
        }
    }

    class HttpRequestExceptionWithStatus : Exception
    {
        public HttpRequestExceptionWithStatus(HttpRequestException inner, HttpStatusCode statusCode)
            : base(inner.Message, inner)
        {
            Exception = inner;
            StatusCode = statusCode;
        }

        public HttpRequestException Exception { get; }
        public HttpStatusCode StatusCode { get; }
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