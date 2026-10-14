using Common;
using MessageBroker_Lab2.Services;

var builder = WebApplication.CreateBuilder(args);

// Configurăm Kestrel să asculte pe portul din clasa Settings folosind HTTP/2
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(Settings.Port, o => o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2);
});

// Adăugăm serviciile gRPC în container
builder.Services.AddGrpc();

var app = builder.Build();

// Mapăm propriul nostru serviciu gRPC în loc de GreeterService
app.MapGrpcService<BrokerService>();

app.MapGet("/", () => "Serverul gRPC Broker este pornit și funcțional!");

app.Run();