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
    Action<HttpClient> configureNewClient;

    public AuthenticatingHttpClient(Action<HttpClient> configureNewClient = null)
    {
        this.configureNewClient = configureNewClient;
        http = CreateHttpClient();
    }

    HttpClient CreateHttpClient(Action<CredentialCache> fillCredentials = null)
    {
        var credentials = new CredentialCache();
        if (fillCredentials is not null)
        {
            fillCredentials(credentials);
        }

        var httpHandler = new HttpClientHandler
        {
            Credentials = fillCredentials is not null ? credentials : null,
            UseDefaultCredentials = fillCredentials is null,
            PreAuthenticate = true,
            AutomaticDecompression = DecompressionMethods.All,
            MaxConnectionsPerServer = 100
        };

        var newClient = new HttpClient(httpHandler, true);
        if (configureNewClient is not null)
        {
            configureNewClient(newClient);
        }
        return newClient;
    }

    public Task<Stream> GetStreamAsync(string url, CancellationToken cancellationToken = default)
    {
        return RetryLoopOnUnauthorized(url, 3, token => HttpGetStreamWithUsefulException(url, token), cancellationToken);
    }

    public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken = default)
    {
        return RetryLoopOnUnauthorized(request.RequestUri, 3, token => http.SendAsync(request, token), cancellationToken);
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
            catch (HttpResponseException x) when (x.StatusCode == HttpStatusCode.Unauthorized)
            {
                if (--tries <= 0)
                {
                    throw;
                }

                var uriPrefix = new Uri(uri.GetLeftPart(UriPartial.Authority));

                Out.WriteLine($"Unable to access {uriPrefix} as {currentUser ?? "default credentials"}.");
                Out.WriteLine("Allowed authentication methods are:");
                foreach (var authHeader in x.Response.Headers.WwwAuthenticate)
                {
                    Out.WriteLine($"  * {authHeader.Scheme} ({authHeader.Parameter})");
                }
                Out.WriteLine();

                Out.WriteLine($"Enter authentication for {uriPrefix}:");
                Out.Write("Username: ");
                var user = Out.ReadLine();
                Out.Write("Password: ");
                var pass = Out.ReadPassword();
                Out.WriteLine();

                var newHttp = CreateHttpClient(credentials =>
                {
                    foreach (var authHeader in x.Response.Headers.WwwAuthenticate)
                    {
                        credentials.Remove(uriPrefix, authHeader.Scheme);
                        credentials.Add(uriPrefix, authHeader.Scheme, new NetworkCredential(user, pass));
                    }
                });

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
            return await content.ReadAsStreamAsync(cancellationToken);
        }
        catch (HttpRequestException x)
        {
            throw new HttpResponseException(x, response);
        }
    }

    class HttpResponseException : Exception
    {
        public HttpResponseException(HttpRequestException inner, HttpResponseMessage response)
            : base(inner.Message, inner)
        {
            Exception = inner;
            StatusCode = response.StatusCode;
            Response = response;
        }

        public HttpRequestException Exception { get; }
        public HttpStatusCode StatusCode { get; }
        public HttpResponseMessage Response { get; }
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