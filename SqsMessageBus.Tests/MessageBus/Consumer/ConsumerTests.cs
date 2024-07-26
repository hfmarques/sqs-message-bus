using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Moq;
using SqsMessageBus.Consumer;
using SqsMessageBus.Publisher;

namespace SqsMessageBus.Tests.MessageBus.Consumer;

public class ConsumerTests
{
    private readonly Mock<ILogger<SqsMessageBus.Consumer.Consumer>> _loggerMock = new();
    private readonly SqsMessageBus.Consumer.Consumer _consumer;
    private readonly Mock<IAmazonSQS> _sqsClientMock = new();
    private readonly Mock<IPublisher> _sender = new();

    public ConsumerTests()
    {
        _consumer = new(_sqsClientMock.Object, _loggerMock.Object, _sender.Object);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldNotLogMessagesReceived_WhenThereIsNoMessages()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<object, Task<Task?>>(_ => Task.FromResult(Task.CompletedTask)!);
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages = []
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _loggerMock.Verify(
            x =>
                x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("Received message from queue")),
                    It.IsAny<Exception>(),
                    ((Func<It.IsAnyType, Exception, string>) It.IsAny<object>())!),
            Times.Never);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldCallOnMessage_WhenThereIsAMessage()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => Task.FromResult(Task.CompletedTask)!);
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _sqsClientMock.Verify(
            x =>
                x.ReceiveMessageAsync(
                    It.Is<ReceiveMessageRequest>(r => r.QueueUrl == queueName),
                    cancellationToken),
            Times.Once);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldSetCorrelationId_WhenObjHasThisField()
    {
        // Arrange
        const string queueName = "test-queue";
        var correlationId = Guid.NewGuid().ToString();
        var cancellationToken = new CancellationToken();

        var onMessage = new Func<TestMessage, Task<Task?>>(message =>
        {
            if (message.CorrelationId != correlationId)
                throw new("CorrelationId is not set");

            return Task.FromResult(Task.CompletedTask)!;
        });

        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageAttributes = new()
                    {
                        {
                            "CorrelationId",
                            new()
                            {
                                DataType = "String",
                                StringValue = correlationId
                            }
                        }
                    }
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);
        _sqsClientMock.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), cancellationToken))
            .ReturnsAsync(new SendMessageResponse
            {
                MessageId = "test-message-id"
            });

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _sqsClientMock.Verify(
            x =>
                x.SendMessageAsync(
                    It.IsAny<SendMessageRequest>(),
                    cancellationToken),
            Times.Never);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldCallSendAsync_WhenThrowExceptionAndErrorQueueName()
    {
        // Arrange
        const string queueName = "test-queue";
        const string errorQueueName = "error-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new());
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);
        _sqsClientMock.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), cancellationToken))
            .ReturnsAsync(new SendMessageResponse
            {
                MessageId = "test-message-id"
            });

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1)
            .WithErrorQueue(errorQueueName);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _sender.Verify(
            x =>
                x.ExecuteAsync(It.IsAny<object>(), It.Is<PublisherParams>(r => r.Queue == errorQueueName), It.IsAny<CancellationToken>()),
            Times.Once);
    }


    [Fact]
    public async Task ReceiveObjectAsync_ShouldNotCallSendAsync_WhenThrowExceptionAndQueueErrorIsEmpty()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new());
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);
        _sqsClientMock.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), cancellationToken))
            .ReturnsAsync(new SendMessageResponse
            {
                MessageId = "test-message-id"
            });

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _sender.Verify(
            x =>
                x.ExecuteAsync(It.IsAny<object>(), It.IsAny<PublisherParams>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldCallDeleteMessage_WhenThrowExceptionAndRemoveFromQueueOnExceptionIsTrue()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new());
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1)
            .WithRemoveFromQueueOnException();

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);


        // Assert
        _sqsClientMock.Verify(
            x =>
                x.DeleteMessageAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    cancellationToken),
            Times.Once);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldNotCallDeleteMessage_WhenThrowTestExceptionAndRemoveFromQueueOnErrorIsTrue()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new TestException());
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1)
            .WithRemoveFromQueueOnException()
            .WithExceptionTypeToDoNotRemoveFromQueue([typeof(TestException)]);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _sqsClientMock.Verify(
            x =>
                x.DeleteMessageAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    cancellationToken),
            Times.Never);
    }
    
    [Fact]
    public async Task ReceiveObjectAsync_ShouldLogWarning_WhenThrowTestException()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new TestException());
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1)
            .WithExceptionTypeToDoNotRemoveFromQueue([typeof(TestException)]);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _loggerMock.Verify(
            x =>
                x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("Expected error processing message")),
                    It.IsAny<Exception>(),
                    ((Func<It.IsAnyType, Exception, string>) It.IsAny<object>())!),
            Times.Once);
    }
    
    [Fact]
    public async Task ReceiveObjectAsync_ShouldLogError_WhenThrowUnknownException()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new ArgumentException());
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1)
            .WithExceptionTypeToDoNotRemoveFromQueue([typeof(TestException)]);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _loggerMock.Verify(
            x =>
                x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("Error processing message")),
                    It.IsAny<Exception>(),
                    ((Func<It.IsAnyType, Exception, string>) It.IsAny<object>())!),
            Times.Once);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldNotDeleteMessage_WhenThrowTestExceptionAndRemoveFromQueueOnErrorIsFalse()
    {
        // Arrange
        const string queueName = "test-queue";
        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new TestException());
        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id"
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1)
            .WithExceptionTypeToDoNotRemoveFromQueue([typeof(TestException)]);

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken: cancellationToken);

        // Assert
        _sqsClientMock.Verify(
            x =>
                x.DeleteMessageAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    cancellationToken),
            Times.Never);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldCallSend_WhenMessageHasReplyTo()
    {
        // Arrange
        const string queueName = "test-queue";

        async Task<ReturnMessage?> OnMessage(TestMessage input)
        {
            await Task.CompletedTask;
            return new()
            {
                Id = 123,
                CorrelationId = Guid.NewGuid().ToString()
            };
        }

        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id",
                    MessageAttributes = new()
                    {
                        {
                            "ReplyTo",
                            new()
                            {
                                DataType = "String",
                                StringValue = "test-reply-to"
                            }
                        }
                    }
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1);

        // Act
        await _consumer.ExecuteAsync<TestMessage, ReturnMessage>(OnMessage, consumerParameters, cancellationToken);

        // Assert
        _sender.Verify(
            x =>
                x.ExecuteAsync(
                    It.IsAny<object>(),
                    It.Is<PublisherParams>(y => y.Queue == "test-reply-to"),
                    It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ReceiveObjectAsync_ShouldKeepReplyTo_WhenOnMessageThrowsException()
    {
        // Arrange
        const string queueName = "test-queue";

        var onMessage = new Func<TestMessage, Task<Task?>>(_ => throw new TestException());


        var cancellationToken = new CancellationToken();
        var response = new ReceiveMessageResponse
        {
            Messages =
            [
                new()
                {
                    Body =
                        """
                            { "Id": 1 }
                        """,
                    MessageId = "test-message-id",
                    MessageAttributes = new()
                    {
                        {
                            "ReplyTo",
                            new()
                            {
                                DataType = "String",
                                StringValue = "test-reply-to"
                            }
                        }
                    }
                }
            ]
        };

        _sqsClientMock.Setup(x => x.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), cancellationToken))
            .ReturnsAsync(response);

        var consumerParameters = new ConsumerParams()
            .WithQueue(queueName)
            .WithMaxNumberOfMessages(1)
            .WithWaitTimeSeconds(1)
            .WithErrorQueue("test-error-queue");

        // Act
        await _consumer.ExecuteAsync(onMessage, consumerParameters, cancellationToken);

        // Assert
        _sender.Verify(
            x =>
                x.ExecuteAsync(
                    It.IsAny<object>(),
                    It.Is<PublisherParams>(y => y.ReplyTo == "test-reply-to"),
                    It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
public class TestMessage
{
    public long Id { get; set; }
    public string? CorrelationId { get; set; }
}
public class ReturnMessage
{
    public long Id { get; set; }
    public string? CorrelationId { get; set; }
}
public class TestException : Exception
{
}