using System.Security.Cryptography;
using System.Text;
using LicenseTool.Data;
using Newtonsoft.Json;

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
            rsa.ImportFromPem(publicKeyText);

            var hash = sha.ComputeHash(bytesToSign);

            var signature = rsa.Encrypt(hash, RSAEncryptionPadding.Pkcs1);

            return Convert.ToBase64String(signature);
        }
    }
}