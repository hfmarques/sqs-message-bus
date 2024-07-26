namespace SqsMessageBus.Publisher;

public class PublisherParams
{
    public string Queue { get; private set; } = default!;
    public PublisherParams WithQueue(string queue)
    {
        Queue = queue;
        return this;
    }
    
    public string? CorrelationId { get; private set; }
    public PublisherParams WithCorrelationId(string correlationId)
    {
        CorrelationId = correlationId;
        return this;
    }

    public string? ReplyTo { get; set; }
    public PublisherParams WithReplyTo(string replyTo)
    {
        ReplyTo = replyTo;
        return this;
    }
}