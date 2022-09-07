# Particular.EndpointThroughputCounter

A tool to measure per-endpoint throughput of an NServiceBus system.

## RSA key pair

The app uses a public/private key pair to prevent tampering with the reports returned from end users. The body of the report is serialized with JSON.NET as unindented, then hashed, and then the hash is encrypted with the private key. The `SignedReport` type then contains the report data along with the Base64-encoded signature, which is JSON-serialized (with indenting for readability) to a file.

The file is verified by deserializing the JSON, then repeating the parts of the process, but ensuring that the decrypted signature matches the correct hash of the data.

It would technically be possible for a user to decompile this code and submit whatever report they want with a valid signature. That is unavoidable as long as the user can use the tool on their own system. The goal is to make it inconvenient enough to do so that most won't try. Even the mere presence of a cryptographic signature in the report is enough of a psychological deterrent from making subtle changes to the report text.

### Generating a key pair

The key pair can be trivially regenerated in case the private key is exposed, becuase we only need to trust the latest version of the tool. Follow these steps:

1. Generate a private key:
   ```
   openssl genrsa -out private-key.pem 2048
   ```
2. Extract the public key:
   ```
   openssl rsa -in private-key.pem -RSAPublicKey_out -out public-key.pem
   ```
3. The public key file is embedded with the tool as an embedded resource.
4. The new private and public keys should be stored in LastPass.
5. Any app needing to verify report files must be configured with the private key as an environment variable.

