namespace Particular.EndpointThroughputCounter.Infra
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;

    static class InteractiveHttpAuth
    {
        public static Task<Func<HttpClient>> CreateHttpClientFactory(string authUrl, int maxTries = 3, Action<HttpClient> configureNewClient = null, CancellationToken cancellationToken = default)
            => CreateHttpClientFactory(new Uri(authUrl), maxTries, configureNewClient, cancellationToken);

        public static async Task<Func<HttpClient>> CreateHttpClientFactory(Uri authUri, int maxTries = 3, Action<HttpClient> configureNewClient = null, CancellationToken cancellationToken = default)
        {
            Uri uriPrefix = null;

            try
            {
                uriPrefix = new Uri(authUri.GetLeftPart(UriPartial.Authority));
            }
            catch (UriFormatException)
            {
                throw new HaltException(HaltReason.InvalidConfig, $"The URL '{authUri}' is invalid. It must be fully-formed, including http:// or https://.");
            }

            var credentials = new CredentialCache();

            NetworkCredential credential = null;
            var schemes = Array.Empty<string>();
            string currentUser = null;

            while (true)
            {
                var socketHandler = new SocketsHttpHandler
                {
                    PreAuthenticate = true,
                    AutomaticDecompression = DecompressionMethods.All,
                    MaxConnectionsPerServer = 20,
                    PooledConnectionLifetime = TimeSpan.FromMinutes(1)
                };

                if (credential is not null)
                {
                    var credentialCache = new CredentialCache();
                    if (schemes.Any())
                    {
                        foreach (var scheme in schemes)
                        {
                            credentialCache.Add(uriPrefix, scheme, credential);
                        }
                    }
                    else
                    {
                        credentialCache.Add(uriPrefix, "Basic", credential);
                    }
                    socketHandler.Credentials = credentialCache;
                }

                var http = new HttpClient(socketHandler, disposeHandler: false);
                configureNewClient?.Invoke(http);

                try
                {
                    using var response = await http.GetAsync(authUri, cancellationToken);

                    try
                    {
                        _ = response.EnsureSuccessStatusCode();

                        return () => new HttpClient(socketHandler, disposeHandler: false);
                    }
                    catch (HttpRequestException x) when (response.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        socketHandler.Dispose();
                        if (--maxTries <= 0)
                        {
                            throw new HaltException(HaltReason.Auth, $"Unable to authenticate to {uriPrefix}", x);
                        }

                        Out.WriteLine();
                        Out.WriteLine($"Unable to access {uriPrefix} as {currentUser ?? "default credentials"}.");
                        Out.WriteLine("Allowed authentication methods are:");
                        foreach (var authHeader in response.Headers.WwwAuthenticate)
                        {
                            Out.WriteLine($"  * {authHeader.Scheme} ({authHeader.Parameter})");
                        }
                        Out.WriteLine();

                        Out.WriteLine($"Enter authentication for {uriPrefix}:");
                        Out.Write("Username: ");
                        currentUser = Out.ReadLine();
                        Out.Write("Password: ");
                        var pass = Out.ReadPassword();

                        credential = new NetworkCredential(currentUser, pass);
                        var newSchemes = response.Headers.WwwAuthenticate.Select(h => h.Scheme).ToArray();
                        if (newSchemes.Any())
                        {
                            schemes = newSchemes;
                        }
                    }
                }
                catch (HttpRequestException x)
                {
                    throw new HaltException(HaltReason.InvalidConfig, $"Unable to connect to '{authUri}'. Are you sure you have the correct URL? Original error message was: {x.Message}");
                }
            }
        }
    }
}