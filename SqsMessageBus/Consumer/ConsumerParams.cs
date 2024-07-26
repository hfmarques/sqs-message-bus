namespace SqsMessageBus.Consumer;

public class ConsumerParams
{
    public string Queue { get; private set; } = default!;
    public ConsumerParams WithQueue(string queue)
    {
        Queue = queue;
        return this;
    }

    public int WaitTimeSeconds { get; set; } = 10;
    public ConsumerParams WithWaitTimeSeconds(int waitTimeSeconds)
    {
        WaitTimeSeconds = waitTimeSeconds;
        return this;
    }
    
    public int MaxNumberOfMessages { get; set; } = 10;
    public ConsumerParams WithMaxNumberOfMessages(int maxNumberOfMessages)
    {
        MaxNumberOfMessages = maxNumberOfMessages;
        return this;
    }
    
    public string? ErrorQueue { get; set; }
    public ConsumerParams WithErrorQueue(string errorQueue)
    {
        ErrorQueue = errorQueue;
        return this;
    }
    
    public string? ReplyTo { get; set; }
    public ConsumerParams WithReplyTo(string replyTo)
    {
        ReplyTo = replyTo;
        return this;
    }
    
    public bool RemoveFromQueueOnException { get; set; } = false;
    public ConsumerParams WithRemoveFromQueueOnException()
    {
        RemoveFromQueueOnException = true;
        return this;
    }

    public List<Type> ExceptionTypeToDoNotRemoveFromQueue { get; set; } = [];
    public ConsumerParams WithExceptionTypeToDoNotRemoveFromQueue(List<Type> exceptionTypeToDoNotRemoveFromQueue)
    {
        ExceptionTypeToDoNotRemoveFromQueue = exceptionTypeToDoNotRemoveFromQueue;
        return this;
    }
}