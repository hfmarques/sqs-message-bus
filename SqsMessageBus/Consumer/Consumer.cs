using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Dawn;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using SqsMessageBus.Publisher;

namespace SqsMessageBus.Consumer;

public interface IConsumer
{
    Task ExecuteAsync<TRequest, TResponse>(Func<TRequest, Task<TResponse?>> onMessage, ConsumerParams consumerParams, CancellationToken cancellationToken = default) where TRequest : class where TResponse : class;
}
public class Consumer(IAmazonSQS sqsClient, ILogger<Consumer> logger, IPublisher publisher) : IConsumer
{

    public async Task ExecuteAsync<TRequest, TResponse>(
        Func<TRequest, Task<TResponse?>> onMessage,
        ConsumerParams consumerParams,
        CancellationToken cancellationToken = default)
        where TRequest : class where TResponse : class
    {
        Guard.Argument(consumerParams).NotNull();
        Guard.Argument(consumerParams.Queue).NotNull().NotEmpty();
        Guard.Argument(consumerParams.WaitTimeSeconds).NotNegative();
        Guard.Argument(consumerParams.MaxNumberOfMessages).NotNegative();

        var request = new ReceiveMessageRequest
        {
            QueueUrl = consumerParams.Queue,
            WaitTimeSeconds = consumerParams.WaitTimeSeconds,
            MaxNumberOfMessages = consumerParams.MaxNumberOfMessages,
            AttributeNames = ["All"],
            MessageAttributeNames = ["All"]
        };

        var response = await sqsClient.ReceiveMessageAsync(request, cancellationToken);
        if (response.Messages.Count == 0)
        {
            // _logger.LogInformation("No messages to be received on queue {Queue}", consumerParams.Queue);
            return;
        }

        foreach (var message in response.Messages)
        {
            await ReceiveMessage(onMessage, consumerParams, message, cancellationToken);
        }
    }

    private async Task ReceiveMessage<TRequest, TResponse>(
        Func<TRequest, Task<TResponse?>> handler,
        ConsumerParams consumerParams,
        Message message,
        CancellationToken cancellationToken)
    where TRequest : class where TResponse : class
    {
        message.MessageAttributes.TryGetValue("CorrelationId", out var messageGroupIdAttribute);
        var correlationId = messageGroupIdAttribute?.StringValue ?? Guid.NewGuid().ToString();
        message.MessageAttributes.TryGetValue("ReplyTo", out var replyToMessageAttributeValue);
        var replyTo = replyToMessageAttributeValue?.StringValue ?? consumerParams.ReplyTo;

        using var _ = LogContext.PushProperty("CorrelationId", correlationId);

        logger.LogInformation("Received message from queue {Queue} with id {MessageId}", consumerParams.Queue, message.MessageId);
        try
        {
            var obj = JsonSerializer.Deserialize<TRequest>(message.Body);

            if (obj?.GetType().GetProperty("CorrelationId") is not null)
                obj.GetType().GetProperty("CorrelationId")!.SetValue(obj, correlationId);

            var result = await handler.Invoke(obj!);

            await sqsClient.DeleteMessageAsync(consumerParams.Queue, message.ReceiptHandle, cancellationToken);

            if (!string.IsNullOrEmpty(replyTo))
            {
                await Reply(replyTo, correlationId, result, cancellationToken);
            }
        }
        catch (Exception e)
        {
            await HandleException(consumerParams, message, e, correlationId, replyTo, cancellationToken);
        }
    }

    private async Task Reply<TResponse>(
        string replyTo,
        string correlationId,
        TResponse? result,
        CancellationToken cancellationToken)
        where TResponse : class
    {
        var senderParameters = new PublisherParams()
            .WithQueue(replyTo)
            .WithCorrelationId(correlationId);

        logger.LogInformation("Replying to queue: {QueueName}", replyTo);

        if (result is not null)
            await publisher.ExecuteAsync(result, senderParameters, cancellationToken);
    }


    private async Task HandleException(
        ConsumerParams consumerParams,
        Message message,
        Exception e,
        string correlationId,
        string? replyTo,
        CancellationToken cancellationToken)
    {
        if (consumerParams.ExceptionTypeToDoNotRemoveFromQueue.Contains(e.GetType()))
            logger.LogWarning("Expected error processing message {MessageId} \n{Exception}", message.MessageId, e.ToString());
        else
            logger.LogError("Error processing message {MessageId} \n{Exception}", message.MessageId, e.ToString());

        if (consumerParams.RemoveFromQueueOnException
            && (consumerParams.ExceptionTypeToDoNotRemoveFromQueue.Count == 0 || !consumerParams.ExceptionTypeToDoNotRemoveFromQueue.Contains(e.GetType())))
        {
            await sqsClient.DeleteMessageAsync(consumerParams.Queue, message.ReceiptHandle, cancellationToken);
        }

        if (!string.IsNullOrEmpty(consumerParams.ErrorQueue))
        {
            var senderParameters = new PublisherParams()
                .WithQueue(consumerParams.ErrorQueue)
                .WithCorrelationId(correlationId);

            if(!string.IsNullOrWhiteSpace(replyTo))
                senderParameters.WithReplyTo(replyTo);

            await publisher.ExecuteAsync(message.Body, senderParameters, cancellationToken: cancellationToken);
        }
    }
}
