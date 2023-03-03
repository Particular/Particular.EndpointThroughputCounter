#if NET472
namespace Particular.ThroughputQuery
{
    using System.IO;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;


    public static class NetFrameworkPolyfills
    {
        public static Task<Stream> GetStreamAsync(this HttpClient http, string requestUri, CancellationToken cancellationToken = default)
        {
            return http.GetStreamAsync(requestUri);
        }
    }
}
#endif
