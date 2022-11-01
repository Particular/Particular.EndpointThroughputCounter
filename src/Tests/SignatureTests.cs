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
                    using var rsa = RSA.Create();

                    ImportPrivateKey(rsa, Environment.GetEnvironmentVariable("RSA_PRIVATE_KEY"));

                    var correctSignature = Convert.ToBase64String(shaHash);

                    var decryptedHash = rsa.Decrypt(Convert.FromBase64String(signedReport.Signature), RSAEncryptionPadding.Pkcs1);
                    var decryptedSignature = Convert.ToBase64String(decryptedHash);

                    result = correctSignature == decryptedSignature;
                }
                catch (CryptographicException x)
                {
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
            rsa.ImportFromPem(privateKeyText);
#else
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

            var bytes = Convert.FromBase64String(base64Builder.ToString());

            rsa.ImportRSAPrivateKey(bytes, out int bytesRead);

            if (bytesRead != bytes.Length)
            {
                throw new Exception("Failed to read public key.");
            }
#endif
        }
    }
}
