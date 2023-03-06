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
        public static Task<HttpClient> CreateHttpClient(string authUrl, int maxTries = 3, Action<HttpClient> configureNewClient = null, CancellationToken cancellationToken = default)
            => CreateHttpClient(new Uri(authUrl), maxTries, configureNewClient, cancellationToken);

        public static async Task<HttpClient> CreateHttpClient(Uri authUri, int maxTries = 3, Action<HttpClient> configureNewClient = null, CancellationToken cancellationToken = default)
        {
            var uriPrefix = new Uri(authUri.GetLeftPart(UriPartial.Authority));
            var credentials = new CredentialCache();

            NetworkCredential credential = null;
            var schemes = Array.Empty<string>();
            string currentUser = null;

            while (true)
            {
                var httpHandler = new HttpClientHandler
                {
                    PreAuthenticate = true,
                    AutomaticDecompression = DecompressionMethods.All,
                    MaxConnectionsPerServer = 100
                };

                if (schemes.Any() && credential is not null)
                {
                    var credentialCache = new CredentialCache();
                    foreach (var scheme in schemes)
                    {
                        credentialCache.Add(uriPrefix, scheme, credential);
                    }
                    httpHandler.Credentials = credentialCache;
                }
                else
                {
                    httpHandler.UseDefaultCredentials = true;
                }

                var http = new HttpClient(httpHandler, true);
                configureNewClient?.Invoke(http);

                using var response = await http.GetAsync(authUri, cancellationToken);

                try
                {
                    _ = response.EnsureSuccessStatusCode();
                    return http;
                }
                catch (HttpRequestException x) when (response.StatusCode == HttpStatusCode.Unauthorized)
                {
                    http.Dispose();
                    if (--maxTries <= 0)
                    {
                        throw new HaltException(HaltReason.Auth, "Unable to authenticate to " + uriPrefix, x);
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
                    schemes = response.Headers.WwwAuthenticate.Select(h => h.Scheme).ToArray();
                }
            }
        }
    }
}