using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;
using Particular.EndpointThroughputCounter.Data;

static class Signature
{
    static readonly string publicKeyText;

    static Signature()
    {
        var assembly = typeof(Signature).Assembly;
        var assemblyName = assembly.GetName().Name;
        using (var stream = assembly.GetManifestResourceStream($"{assemblyName}.public-key.pem"))
        using (var reader = new StreamReader(stream))
        {
            publicKeyText = reader.ReadToEnd();
        }
    }

    public static string SignReport(Report report)
    {
        var jsonToSign = JsonConvert.SerializeObject(report, Formatting.None);

        var bytesToSign = Encoding.UTF8.GetBytes(jsonToSign);

        using (var rsa = RSA.Create())
        using (var sha = SHA512.Create())
        {
            ImportPublicKey(rsa, publicKeyText);

            var hash = sha.ComputeHash(bytesToSign);

            var signature = rsa.Encrypt(hash, RSAEncryptionPadding.Pkcs1);

            return Convert.ToBase64String(signature);
        }
    }

    static void ImportPublicKey(RSA rsa, string publicKeyText)
    {
#if NET5_0_OR_GREATER
        rsa.ImportFromPem(publicKeyText);
#else
        var base64Builder = new StringBuilder();
        using (var reader = new StringReader(publicKeyText))
        {
            while (true)
            {
                var line = reader.ReadLine();
                if (line == null)
                {
                    break;
                }
                if (line.StartsWith("---"))
                {
                    continue;
                }
                base64Builder.Append(line.Trim());
            }
        }

        var bytes = Convert.FromBase64String(base64Builder.ToString());

        rsa.ImportRSAPublicKey(bytes, out int bytesRead);

        if (bytesRead != bytes.Length)
        {
            throw new Exception("Failed to read public key.");
        }
#endif
    }
}