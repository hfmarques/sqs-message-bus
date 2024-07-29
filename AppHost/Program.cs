var builder = DistributedApplication.CreateBuilder(args);

var localstack = builder
    .AddContainer("localstack", "localstack/localstack")
    .WithHttpEndpoint(port: 4566, targetPort: 4566, "http")
    .WithEnvironment("AWS_DEFAULT_REGION", "us-east-1")
    .WithEnvironment("AWS_ACCESS_KEY_ID", "key")
    .WithEnvironment("AWS_SECRET_ACCESS_KEY", "secret")
    .WithEnvironment("SERVICES", "sqs,s3")
    .WithEnvironment("DOCKER_HOST", "unix:///var/run/docker.sock")
    .WithEnvironment("PERSISTENCE", "1")
    // .WithBindMount("/volume", "/var/lib/localstack", isReadOnly: false)
    .WithBindMount("../SqsMessageBus/aws","/etc/localstack/init/ready.d", isReadOnly: true);

var webapi = builder.AddProject<Projects.WebApi>("webapi")
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
    .WithEnvironment("AWS:Sqs:ServiceUrl", localstack.GetEndpoint("http"))
    .WithExternalHttpEndpoints();

builder.Build().Run();