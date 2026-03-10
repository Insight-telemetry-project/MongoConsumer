using MongoConsumer.Models.Configuration;
using MongoConsumer.Models.Interface;
using MongoConsumer.Services.Application;
using MongoConsumer.Services.Kafka;
using MongoConsumer.Services.Network;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.Configure<MongoSettings>(
    builder.Configuration.GetSection(MongoSettings.SectionName));

builder.Services.AddOpenApi();

builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
builder.Services.AddSingleton<ITelemetryRepository, FlightTelemetryMongoProxy>();
builder.Services.AddSingleton<ApplicationStartup>();

builder.Services.AddHttpClient<IFlightAnalysisTriggerService, FlightAnalysisTriggerService>(client =>
{
    client.BaseAddress = new Uri("https://localhost:7274/");
});
WebApplication app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.UseCors("AllowAngularApp");

app.MapControllers();

ApplicationStartup startup = app.Services.GetRequiredService<ApplicationStartup>();
startup.RegisterApplicationEvents();

app.Run();
