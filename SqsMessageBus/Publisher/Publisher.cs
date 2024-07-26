using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Dawn;
using Microsoft.Extensions.Logging;

namespace SqsMessageBus.Publisher;

public interface IPublisher
{
    Task<string> ExecuteAsync<T>(
        T message,
        PublisherParams publisherParams,
        CancellationToken cancellationToken = default) where T : class;
}
public class Publisher(IAmazonSQS sqsClient, ILogger<Publisher> logger) : IPublisher
{

    public async Task<string> ExecuteAsync<T>(
        T message,
        PublisherParams publisherParams,
        CancellationToken cancellationToken = default) where T : class
    {
        Guard.Argument(message, nameof(message)).NotNull();
        Guard.Argument(publisherParams, nameof(publisherParams)).NotNull();
        Guard.Argument(publisherParams.Queue, nameof(publisherParams.Queue)).NotNull().NotEmpty();

        var correlationId = publisherParams.CorrelationId ?? Guid.NewGuid().ToString();

        using var _ = logger.BeginScope(new Dictionary<string, object> {{"CorrelationId", correlationId}});

        var request = new SendMessageRequest
        {
            QueueUrl = publisherParams.Queue,
            MessageBody = message is string ? message.ToString() : JsonSerializer.Serialize(message),
            MessageAttributes = new()
            {
                {"CorrelationId", new()
                    {StringValue = correlationId, DataType = "String"}}
            }
        };
        
        if(!string.IsNullOrWhiteSpace(publisherParams.ReplyTo))
            request.MessageAttributes.Add("ReplyTo", new()
                {StringValue = publisherParams.ReplyTo, DataType = "String"});
        
        var response = await sqsClient.SendMessageAsync(request, cancellationToken);

        logger.LogInformation("Message sent to queue {QueueUrl} with id {MessageId}", publisherParams.Queue, response.MessageId);

        return correlationId;
    }
}