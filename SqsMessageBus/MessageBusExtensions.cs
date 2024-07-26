using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Dawn;
using Microsoft.Extensions.DependencyInjection;
using SqsMessageBus.Consumer;
using SqsMessageBus.Publisher;

namespace SqsMessageBus;

public static class MessageBusExtensions
{
    public static void AddServicesFromMessageBus(
        this IServiceCollection services,
        MessageBusExtensionsParams messageBusExtensionsParams)
    {
        Guard.Argument(messageBusExtensionsParams).NotNull();
        Guard.Argument(messageBusExtensionsParams.AuthenticationRegion).NotNull().NotEmpty();
        
        AWSCredentials awsCredentials;
        if (messageBusExtensionsParams.FallbackCredentialsFactory)
        {
            awsCredentials = FallbackCredentialsFactory.GetCredentials();
        }
        else
        {
            awsCredentials = new BasicAWSCredentials(
                messageBusExtensionsParams.AccessKey, 
                messageBusExtensionsParams.SecretKey);
        }
        
        if (messageBusExtensionsParams.FallbackCredentialsFactory)
        {
            if(messageBusExtensionsParams.Singleton)
            {
                services.AddSingleton<IAmazonSQS>(
                    _ => new AmazonSQSClient(
                        awsCredentials,
                        RegionEndpoint.GetBySystemName(messageBusExtensionsParams.AuthenticationRegion)));
                services.AddSingleton<IConsumer, Consumer.Consumer>();
                services.AddSingleton<IPublisher, Publisher.Publisher>();
            }
            else
            {
                services.AddTransient<IAmazonSQS>(
                    _ => new AmazonSQSClient(
                        awsCredentials,
                        RegionEndpoint.GetBySystemName(messageBusExtensionsParams.AuthenticationRegion)));
                services.AddTransient<IConsumer, Consumer.Consumer>();
                services.AddTransient<IPublisher, Publisher.Publisher>();
            }
        }
        else if (messageBusExtensionsParams.CredentialsAuthentication)
        {
            if (messageBusExtensionsParams.Singleton)
            {
                services.AddSingleton<IAmazonSQS>(
                    _ => new AmazonSQSClient(
                        awsCredentials,
                        RegionEndpoint.GetBySystemName(messageBusExtensionsParams.AuthenticationRegion)
                    )
                );
                services.AddSingleton<IConsumer, Consumer.Consumer>();
                services.AddSingleton<IPublisher, Publisher.Publisher>();
            }
            else
            {
                services.AddTransient<IAmazonSQS>(
                    _ => new AmazonSQSClient(
                        awsCredentials,
                        RegionEndpoint.GetBySystemName(messageBusExtensionsParams.AuthenticationRegion)
                    )
                );
                services.AddTransient<IConsumer, Consumer.Consumer>();
                services.AddTransient<IPublisher, Publisher.Publisher>();
            }
        }
        else
        {
            if (messageBusExtensionsParams.Singleton)
            {
                services.AddSingleton<IAmazonSQS>(
                    _ => new AmazonSQSClient(
                        awsCredentials,
                        new AmazonSQSConfig
                        {
                            AuthenticationRegion = messageBusExtensionsParams.AuthenticationRegion,
                            ServiceURL = messageBusExtensionsParams.SqsServiceUrl
                        }
                    )
                );
                services.AddSingleton<IConsumer, Consumer.Consumer>();
                services.AddSingleton<IPublisher, Publisher.Publisher>();
            }
            else
            {
                services.AddTransient<IAmazonSQS>(
                    _ => new AmazonSQSClient(
                        awsCredentials,
                        new AmazonSQSConfig
                        {
                            AuthenticationRegion = messageBusExtensionsParams.AuthenticationRegion,
                            ServiceURL = messageBusExtensionsParams.SqsServiceUrl
                        }
                    )
                );
                services.AddTransient<IConsumer, Consumer.Consumer>();
                services.AddTransient<IPublisher, Publisher.Publisher>();
            }
        }
    }
}