using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Moq;
using SqsMessageBus.Publisher;

namespace SqsMessageBus.Tests.MessageBus.Publisher;

public class PublisherTests
{
    private readonly Mock<ILogger<SqsMessageBus.Publisher.Publisher>> _loggerMock = new();
    private readonly Mock<IAmazonSQS> _sqsClientMock = new();
    private readonly SqsMessageBus.Publisher.Publisher _publisher;

    public PublisherTests()
    {
        _publisher = new(_sqsClientMock.Object, _loggerMock.Object);
    }
    
    [Fact]
    public async Task SendAsync_ShouldCallSendMessageAsync()
    {
        // Arrange
        const string queueName = "test-queue";
        const string message = "test-message";
        var cancellationToken = new CancellationToken();
        var response = new SendMessageResponse
        {
            MessageId = "test-message-id"
        };

        _sqsClientMock.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);
        
        var senderParameters = new PublisherParams()
            .WithQueue(queueName);
        
        // Act
        await _publisher.ExecuteAsync(message, senderParameters, cancellationToken);

        // Assert
        _sqsClientMock.Verify(
            x =>
                x.SendMessageAsync(
                    It.Is<SendMessageRequest>(r => r.QueueUrl == queueName && r.MessageBody == message),
                    cancellationToken),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_ShouldThrowException_WhenMessageIsEmpty()
    {
        // Arrange
        const string queueName = "test-queue";
        const string message = "";
        var cancellationToken = new CancellationToken();
        
        var senderParameters = new PublisherParams()
            .WithQueue(queueName);

        // Act
        await Assert.ThrowsAsync<NullReferenceException>(
            () => _publisher.ExecuteAsync(message, senderParameters, cancellationToken));
    }
    
    [Fact]
    public async Task SendAsync_ShouldThrowException_WhenSenderParametersIsNull()
    {
        // Arrange
        const string message = "aaa";
        var cancellationToken = new CancellationToken();

        var senderParameters = new PublisherParams();

        // Act
        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => _publisher.ExecuteAsync(message, senderParameters, cancellationToken));
        
        // Assert
        Assert.Equal("Queue cannot be null. (Parameter 'Queue')", exception.Message);
    }

    [Fact]
    public async Task SendAsync_ShouldCallSendMessageAsyncWithTheRightCorrelationIdWhenCorrelationIdIsProvided()
    {
        // Arrange
        const string queueName = "test-queue";
        const string message = "test-message";
        var correlationId = Guid.NewGuid().ToString();
        var cancellationToken = new CancellationToken();
        var response = new SendMessageResponse
        {
            MessageId = correlationId
        };

        _sqsClientMock.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var senderParameters = new PublisherParams()
            .WithQueue(queueName)
            .WithCorrelationId(correlationId);

        // Act
        await _publisher.ExecuteAsync(message, senderParameters, cancellationToken);

        // Assert
        _sqsClientMock.Verify(
            x =>
                x.SendMessageAsync(
                    It.Is<SendMessageRequest>(
                        r => r.MessageAttributes["CorrelationId"].StringValue == correlationId && r.MessageBody == message),
                    cancellationToken),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_ShouldCallSendMessageAsyncWithTheRightReplyToWhenProvided()
    {
        // Arrange
        const string queueName = "test-queue";
        const string message = "test-message";
        const string replyTo = "test-reply-to";
        var cancellationToken = new CancellationToken();
        var response = new SendMessageResponse
        {
            MessageId = Guid.NewGuid().ToString()
        };

        _sqsClientMock.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var senderParameters = new PublisherParams()
            .WithQueue(queueName)
            .WithReplyTo(replyTo);

        // Act
        await _publisher.ExecuteAsync(message, senderParameters, cancellationToken);

        // Assert
        _sqsClientMock.Verify(
            x =>
                x.SendMessageAsync(
                    It.Is<SendMessageRequest>(
                        r => r.MessageAttributes["ReplyTo"].StringValue == replyTo && r.MessageBody == message),
                    cancellationToken),
            Times.Once);
    }
}