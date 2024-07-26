namespace SqsMessageBus;

public class MessageBusExtensionsParams
{
    public bool Singleton { get; private set; }
    public MessageBusExtensionsParams WithSingleton()
    {
        Singleton = true;
        return this;
    }
    
    public bool Transient { get; private set; }
    public MessageBusExtensionsParams WithTransient()
    {
        Transient = true;
        return this;
    }
    
    public string AuthenticationRegion { get; private set; }
    public MessageBusExtensionsParams WithAuthenticationRegion(string authenticationRegion)
    {
        AuthenticationRegion = authenticationRegion;
        return this;
    }
    
    public string SqsServiceUrl { get; private set; }
    public MessageBusExtensionsParams WithSqsServiceUrl(string sqsServiceUrl)
    {
        SqsServiceUrl = sqsServiceUrl;
        return this;
    }

    public bool CredentialsAuthentication { get; private set; }
    public string AccessKey { get; private set; } = "";
    public string SecretKey { get; private set; } = "";
    
    public MessageBusExtensionsParams WithCredentialsAuthentication(string accessKey, string secretKey)
    {
        CredentialsAuthentication = true;
        AccessKey = accessKey;
        SecretKey = secretKey;
        return this;
    }

    public bool FallbackCredentialsFactory { get; private set; }
    public MessageBusExtensionsParams WithFallbackCredentialsFactory()
    {
        FallbackCredentialsFactory = true;
        return this;
    }
}