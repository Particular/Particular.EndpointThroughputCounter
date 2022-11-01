namespace Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Text;
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Particular.Approvals;
    using Particular.EndpointThroughputCounter.Data;

    [TestFixture]
    public class SignatureTests
    {
        [TestCase("Serialized")]
        [TestCase("Deserialized")]
        public void SignatureRoundTrip(string scenario)
        {
            var report = CreateReport();

            var reportString = SerializeReport(report);
            if (scenario == "Serialized")
            {
                Approver.Verify(reportString,
                    scrubber: input => input.Replace(report.Signature, "SIGNATURE"),
                    scenario: scenario);
            }

            if (scenario == "Deserialized")
            {
                var deserialized = DeserializeReport(reportString);

                Approver.Verify(reportString,
                    scrubber: input => input.Replace(report.Signature, "SIGNATURE"),
                    scenario: scenario);

                // We don't distribute the private key to do local testing, this only happens during CI
                if (Environment.GetEnvironmentVariable("CI") != "true")
                {
                    return;
                }

                Assert.IsTrue(ValidateReport(deserialized));
            }
        }

        [Test]
        public void TamperCheck()
        {
            var report = CreateReport();
            var reportString = SerializeReport(report);

            reportString = reportString.Replace("42", "13");

            var deserialized = DeserializeReport(reportString);

            // We don't distribute the private key to do local testing, this only happens during CI
            if (Environment.GetEnvironmentVariable("CI") != "true")
            {
                return;
            }

            Assert.IsFalse(ValidateReport(deserialized));
        }

        [Test]
        public void CanReadV1Report()
        {
            var reportString = GetResource("throughput-report-v1.0.json");

            var report = DeserializeReport(reportString);
            var data = report.ReportData;

            // Want to be explicit with asserts to ensure that a 1.0 report can be read correctly
            // An approval test would be too easy to just accept changes on
            Assert.That(data.CustomerName, Is.EqualTo("Testing"));
            Assert.That(data.MessageTransport, Is.EqualTo("RabbitMQ"));
            Assert.That(data.ReportMethod, Is.EqualTo("ThroughputTool: RabbitMQ Admin"));
            Assert.That(data.ToolVersion, Is.EqualTo("1.0.0"));
            Assert.That(data.StartTime.ToString("O"), Is.EqualTo("2022-11-01T10:58:55.5665172-05:00"));
            Assert.That(data.EndTime.ToString("O"), Is.EqualTo("2022-11-01T10:59:55.6677584-05:00"));
            Assert.That(data.ReportDuration, Is.EqualTo(TimeSpan.Parse("00:01:00.1012412")));

            Assert.That(data.Queues.Length, Is.EqualTo(7));
            Assert.That(data.Queues.All(q => q.Throughput == 0));
            Assert.That(data.Queues.All(q => !string.IsNullOrEmpty(q.QueueName)));

            Assert.That(data.TotalThroughput, Is.EqualTo(0));
            Assert.That(data.TotalQueues, Is.EqualTo(7));

            Assert.That(report.Signature, Is.EqualTo("ybIzoo9ogZtbSm5+jJa3GxncjCX3fxAfiLSI7eogG20KjJiv43aCE+7Lsvhkat7AALM34HgwI3VsgzRmyLYXD5n0+XRrWXNgeRGbLEG6d1W2djLRHNjXo423zpGTYDeMq3vhI9yAcil0K0dCC/ZCnw8dPd51pNmgKYIvrfELW0hyN70trUeCMDhYRfXruWLNe8Hfy+tS8Bm13B5vknXNlAjBIuGjXn3XILRRSVrTbb4QMIRzSluSnSTFPTCyE9wMWwC0BUGSf7ZEA0XdeN6UkaO/5URSOQVesiSLRqQWbfUc87XlY1hMs5Z7kLSOr5WByIQIfQKum1nGVjLMzshyhQ=="));

            // We don't distribute the private key to do local testing, this only happens during CI
            if (Environment.GetEnvironmentVariable("CI") != "true")
            {
                return;
            }

            ValidateReport(report);
        }

        string GetResource(string resourceName)
        {
            var assembly = typeof(SignatureTests).Assembly;
            var assemblyName = assembly.GetName().Name;
            using (var stream = assembly.GetManifestResourceStream($"{assemblyName}.{resourceName}"))
            using (var reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        SignedReport CreateReport()
        {
            var start = new DateTimeOffset(2022, 09, 01, 0, 0, 0, TimeSpan.Zero);
            var end = start.AddDays(1);

            var queues = new[]
            {
                new QueueThroughput { QueueName = "Queue1", Throughput = 42 },
                new QueueThroughput { QueueName = "Queue1", Throughput = 10000 },
                new QueueThroughput { QueueName = "NoData", NoDataOrSendOnly = true },
            };

            var reportData = new Report
            {
                CustomerName = "Test",
                MessageTransport = "Fake Transport",
                ReportMethod = "Testing",
                ToolVersion = "1.0.0",
                StartTime = start,
                EndTime = end,
                ReportDuration = end - start,
                Queues = queues,
                TotalThroughput = queues.Sum(q => q.Throughput ?? 0),
                TotalQueues = queues.Length
            };

            return new SignedReport
            {
                ReportData = reportData,
                Signature = Signature.SignReport(reportData)
            };
        }

        string SerializeReport(SignedReport report)
        {
            var serializer = new JsonSerializer();

            using var writer = new StringWriter();
            using var jsonWriter = new JsonTextWriter(writer);

            jsonWriter.Formatting = Formatting.Indented;
            serializer.Serialize(jsonWriter, report, typeof(SignedReport));
            return writer.ToString();

        }

        SignedReport DeserializeReport(string reportString)
        {
            var serializer = new JsonSerializer();

            using var reader = new StringReader(reportString);
            using var jsonReader = new JsonTextReader(reader);

            return serializer.Deserialize<SignedReport>(jsonReader);

        }

        bool ValidateReport(SignedReport signedReport)
        {
            if (signedReport.Signature is null)
            {
                return false;
            }

            var reserializedReportBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(signedReport.ReportData, Formatting.None));
            var shaHash = GetShaHash(reserializedReportBytes);

            // This check has been a heisenbug on CI, want to see if it's systemic within a single run
            var exceptions = new List<Exception>();
            bool result = false;

            for (var i = 0; i < 3; i++)
            {
                try
                {
                    Console.WriteLine("Creating RSA instance.");
                    using var rsa = RSA.Create();

                    var privateKeyText = Environment.GetEnvironmentVariable("RSA_PRIVATE_KEY");
                    Console.WriteLine($"Loaded private key text, length = {privateKeyText.Length}");
                    ImportPrivateKey(rsa, privateKeyText);

                    Console.WriteLine("Calculating correct signature");
                    var correctSignature = Convert.ToBase64String(shaHash);

                    Console.WriteLine("Decrypting signature with private key");
                    var decryptedHash = rsa.Decrypt(Convert.FromBase64String(signedReport.Signature), RSAEncryptionPadding.Pkcs1);
                    var decryptedSignature = Convert.ToBase64String(decryptedHash);

                    result = correctSignature == decryptedSignature;
                }
                catch (CryptographicException x)
                {
                    Console.WriteLine(x);
                    exceptions.Add(x);
                }
            }

            if (exceptions.Any())
            {
                throw new AggregateException($"Validation has thrown exception on {exceptions.Count}/3 attempts.", exceptions);
            }

            return result;
        }

        byte[] GetShaHash(byte[] reportBytes)
        {
            using var sha = SHA512.Create();

            return sha.ComputeHash(reportBytes);
        }

        static void ImportPrivateKey(RSA rsa, string privateKeyText)
        {
#if NET5_0_OR_GREATER
            Console.WriteLine("Importing private key using .NET5+ ImportFromPem() method");
            rsa.ImportFromPem(privateKeyText);
#else
            Console.WriteLine("Importing private key without .NET 5 APIs - parsing base64 text from pem file");
            var base64Builder = new StringBuilder();
            using (var reader = new StringReader(privateKeyText))
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

            var base64Key = base64Builder.ToString();
            Console.WriteLine($"Private key in base64 is {base64Key.Length} chars");
            var bytes = Convert.FromBase64String(base64Key);
            Console.WriteLine($"Private key is {bytes.Length} bytes");

            Console.WriteLine("Importing private key...");
            rsa.ImportRSAPrivateKey(bytes, out int bytesRead);
            Console.WriteLine($"Private key imported, {bytesRead} bytes read");

            if (bytesRead != bytes.Length)
            {
                throw new Exception("Failed to read public key.");
            }
#endif
        }
    }
}
