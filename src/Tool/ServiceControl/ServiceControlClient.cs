﻿namespace Particular.EndpointThroughputCounter.ServiceControl
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class ServiceControlClient
    {
        static readonly Version MinServiceControlVersion = new Version(4, 21, 8);
        static readonly JsonSerializer serializer = new JsonSerializer();
        static readonly AuthenticatingHttpClient http = new AuthenticatingHttpClient(client => client.Timeout = TimeSpan.FromSeconds(10));

        readonly string rootUrl;
        readonly string paramName;
        readonly string instanceType;

        public ServiceControlClient(string paramName, string instanceType, string rootUrl)
        {
            if (string.IsNullOrWhiteSpace(rootUrl))
            {
                throw new HaltException(HaltReason.InvalidConfig, $"The {paramName} option specifying the {instanceType} URL was not provided.");
            }

            this.paramName = paramName;
            this.instanceType = instanceType;
            this.rootUrl = rootUrl.TrimEnd('/');
        }

        public Task<TJsonType> GetData<TJsonType>(string pathAndQuery, CancellationToken cancellationToken = default)
            where TJsonType : JToken
        {
            return GetData<TJsonType>(pathAndQuery, 1, cancellationToken);
        }

        public async Task<TJsonType> GetData<TJsonType>(string pathAndQuery, int tryCount, CancellationToken cancellationToken = default)
            where TJsonType : JToken
        {
            if (pathAndQuery is null || !pathAndQuery.StartsWith('/'))
            {
                throw new ArgumentException("pathAndQuery must start with a forward slash.");
            }

            var url = rootUrl + pathAndQuery;

            for (int i = 0; i < tryCount; i++)
            {
                try
                {
                    using (var stream = await http.GetStreamAsync(url, cancellationToken))
                    using (var reader = new StreamReader(stream))
                    using (var jsonReader = new JsonTextReader(reader))
                    {
                        return serializer.Deserialize<TJsonType>(jsonReader);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception x)
                {
                    if (i + 1 >= tryCount)
                    {
                        throw new ServiceControlDataException(url, tryCount, x);
                    }
                }
            }

            throw new InvalidOperationException("Retry loop ended without returning or throwing. This should not happen.");
        }

        public async Task CheckEndpoint(Func<string, bool> contentTest, CancellationToken cancellationToken = default)
        {
            HttpResponseMessage res = null;
            try
            {
                res = await http.SendAsync(new HttpRequestMessage(HttpMethod.Get, rootUrl), cancellationToken);
            }
            catch (HttpRequestException hx)
            {
                throw new HaltException(HaltReason.InvalidEnvironment, $"The server at {rootUrl} did not respond. The exception message was: {hx.Message}");
            }

            if (!res.IsSuccessStatusCode)
            {
                var b = new StringBuilder($"The server at {rootUrl} returned a non-successful status code: {(int)res.StatusCode} {res.StatusCode}")
                    .AppendLine()
                    .AppendLine("Response Headers:");

                foreach (var header in res.Headers)
                {
                    _ = b.AppendLine($"  {header.Key}: {header.Value}");
                }

                throw new HaltException(HaltReason.RuntimeError, b.ToString());
            }

            if (!res.Headers.TryGetValues("X-Particular-Version", out var versionHeaders))
            {
                throw new HaltException(HaltReason.InvalidConfig, $"The server at {rootUrl} specified by parameter {paramName} does not appear to be a ServiceControl instance. Are you sure you have the right URL?");
            }

            var version = versionHeaders.Select(header => TryGetVersion(header, out var v) ? v : null).FirstOrDefault();
            Out.WriteLine($"{instanceType} instance at {rootUrl} detected running version {version.ToString(3)}");
            if (version < MinServiceControlVersion)
            {
                throw new HaltException(HaltReason.InvalidEnvironment, $"The {instanceType} instance at {rootUrl} is running version {version.ToString(3)}. The minimum supported version is {MinServiceControlVersion.ToString(3)}.");
            }

            var content = await res.Content.ReadAsStringAsync(cancellationToken);
            if (!contentTest(content))
            {
                throw new HaltException(HaltReason.InvalidConfig, $"The server at {rootUrl} specified by parameter {paramName} does not appear to be a {instanceType} instance. Are you sure you have the right URL?");
            }
        }

        bool TryGetVersion(string versionString, out Version version)
        {
            if (Version.TryParse(versionString, out version))
            {
                return true;
            }

            var firstDash = versionString.IndexOf('-');
            var mainPartIfPrerelease = firstDash >= 0 ? versionString[..firstDash] : null;

            return Version.TryParse(mainPartIfPrerelease, out version);
        }

    }
}
