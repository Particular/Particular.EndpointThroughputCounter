using System;

partial class ServiceControlCommand
{
    class ServiceControlDataException : Exception
    {
        public string Url { get; }
        public int Attempts { get; }

        public ServiceControlDataException(string url, int tryCount, Exception inner)
            : base(inner.Message, inner)
        {
            Url = url;
            Attempts = tryCount;
        }
    }
}