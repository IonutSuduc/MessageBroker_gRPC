using Common;
using MessageBroker_Lab2.Services;

var builder = WebApplication.CreateBuilder(args);

// Preluăm portul direct din clasa centralizată Settings
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(Settings.Port, o =>
    {
        o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2;
        o.UseHttps();
    });
});

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<BrokerService>();
app.MapGet("/", () => "Serverul gRPC Broker HTTPS este pornit!");

app.Run();