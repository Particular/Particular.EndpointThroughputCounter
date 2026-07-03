namespace Tests.SqlServer
{
    using NUnit.Framework;

    [TestFixture]
    public class ConnectionStringSourceTests
    {
        const string SourcePath = "connection-strings.txt";

        [Test]
        public void Parses_valid_lines_and_skips_blank_lines()
        {
            var lines = new[]
            {
                "",
                "Server=srv1;Database=db1",
                "   ",
                "Server=srv2;Database=db2",
                ""
            };

            var result = SqlServerCommand.ParseConnectionStringSource(lines, SourcePath);

            Assert.That(result, Is.EqualTo(new[] { "Server=srv1;Database=db1", "Server=srv2;Database=db2" }));
        }

        [TestCase("\"Server=srv;Database=db\"")]
        [TestCase("'Server=srv;Database=db'")]
        [TestCase("  \"Server=srv;Database=db\"  ")]
        public void Strips_a_single_pair_of_wrapping_quotes(string line)
        {
            var result = SqlServerCommand.ParseConnectionStringSource(new[] { line }, SourcePath);

            Assert.That(result, Is.EqualTo(new[] { "Server=srv;Database=db" }));
        }

        [Test]
        public void Reports_one_based_line_number_and_parse_error_for_invalid_line()
        {
            var lines = new[]
            {
                "Server=srv1;Database=db1",
                "this is not a connection string"
            };

            var halt = Assert.Throws<HaltException>(() => SqlServerCommand.ParseConnectionStringSource(lines, SourcePath));

            Assert.That(halt.ExitCode, Is.EqualTo((int)HaltReason.InvalidConfig));
            Assert.That(halt.Message, Does.Contain("Line 2"));
            Assert.That(halt.Message, Does.Contain(SourcePath));
        }

        [Test]
        public void Reports_invalid_keyword_parse_error()
        {
            var halt = Assert.Throws<HaltException>(() => SqlServerCommand.ParseConnectionStringSource(new[] { "NotAKeyword=value" }, SourcePath));

            Assert.That(halt.ExitCode, Is.EqualTo((int)HaltReason.InvalidConfig));
            Assert.That(halt.Message, Does.Contain("Line 1"));
        }

        [Test]
        public void Throws_when_file_contains_no_connection_strings()
        {
            var halt = Assert.Throws<HaltException>(() => SqlServerCommand.ParseConnectionStringSource(new[] { "", "   " }, SourcePath));

            Assert.That(halt.ExitCode, Is.EqualTo((int)HaltReason.InvalidConfig));
        }
    }

    [TestFixture]
    public class ConnectionStringSanitizerTests
    {
        [TestCase("Server=srv;Database=db;User ID=user;Password=s3cret!")]
        [TestCase("Server=srv;Database=db;User ID=user;Pwd=s3cret!")]
        [TestCase("Server=srv;Database=db;User ID=user;Password='s3cret!;more'")]
        public void Sanitize_blanks_the_password(string connectionString)
        {
            var sanitized = ConnectionStringSanitizer.Sanitize(connectionString);

            Assert.That(sanitized, Does.Not.Contain("s3cret!"));
            Assert.That(sanitized, Does.Contain("srv"));
            Assert.That(sanitized, Does.Contain("user"));
        }

        [Test]
        public void Sanitize_keeps_connection_string_without_password_intact()
        {
            var sanitized = ConnectionStringSanitizer.Sanitize("Server=srv;Database=db;Integrated Security=True");

            Assert.That(sanitized, Does.Contain("srv"));
            Assert.That(sanitized, Does.Not.Contain("*****"));
        }

        [Test]
        public void Sanitize_falls_back_to_redaction_for_unparseable_input()
        {
            var sanitized = ConnectionStringSanitizer.Sanitize("some garbage ;; Password=s3cret!;more garbage");

            Assert.That(sanitized, Does.Not.Contain("s3cret!"));
            Assert.That(sanitized, Does.Contain("*****"));
        }

        [TestCase("Login failed. Connection: Server=x;Password=abc123;Encrypt=true", "abc123")]
        [TestCase("Connection: Server=x;Pwd=abc123;Encrypt=true", "abc123")]
        [TestCase("Connection: Server=x;Password='se;cret';Encrypt=true", "se;cret")]
        [TestCase("Connection: Server=x;Password=\"se;cret\";Encrypt=true", "se;cret")]
        [TestCase("Connection: Server=x; Password = abc123 ;Encrypt=true", "abc123")]
        public void RedactText_masks_secrets_in_free_form_text(string text, string secret)
        {
            var redacted = ConnectionStringSanitizer.RedactText(text);

            Assert.That(redacted, Does.Not.Contain(secret));
            Assert.That(redacted, Does.Contain("*****"));
        }

        [Test]
        public void RedactText_leaves_text_without_secrets_untouched()
        {
            const string text = "SQL error 18456 (state 38) from MYSERVER: Login failed for user 'sa'.";

            Assert.That(ConnectionStringSanitizer.RedactText(text), Is.EqualTo(text));
        }

        [Test]
        public void RedactText_handles_null()
        {
            Assert.That(ConnectionStringSanitizer.RedactText(null), Is.Null);
        }
    }
}
