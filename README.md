# Particular.EndpointThroughputCounter

A tool to measure per-endpoint throughput of an NServiceBus system.

More documentation is [on our website](https://docs.particular.net/nservicebus/throughput-tool/).

## Testing locally

Testing locally requires some configuration that cannot be checked in for security reasons.

Create a `src/Tool/local.settings.json` file that is set to copy to the output directory on build (this is included in .gitignore and will not be checked in), and contains the following:

```json
{
  "AZURESERVICEBUS_RESOURCE_ID": "Your own Azure Service Bus Resource Id"
}
```

* `AZURESERVICEBUS_RESOURCE_ID`: For testing with Azure Service Bus. See the [tool documentation](https://docs.particular.net/nservicebus/throughput-tool/azure-service-bus#running-the-tool) for instructions on how to locate this id.

## RSA key pair

The app uses a public/private key pair to prevent tampering with the reports returned from end users. The body of the report is serialized with JSON.NET as unindented, then hashed, and then the hash is encrypted with the private key. The `SignedReport` type then contains the report data along with the Base64-encoded signature, which is JSON-serialized (with indenting for readability) to a file.

The file is verified by deserializing the JSON, then repeating the parts of the process, but ensuring that the decrypted signature matches the correct hash of the data.

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

## Releasing

Tag the main branch with a new version number to trigger the release build.

