using MongoConsumer.Models.Configuration;
using MongoConsumer.Models.Interface;
using MongoConsumer.Services.Application;
using MongoConsumer.Services.Kafka;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.Configure<MongoSettings>(
    builder.Configuration.GetSection(MongoSettings.SectionName));

builder.Services.AddOpenApi();

builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
builder.Services.AddSingleton<ITelemetryRepository, TelemetryRepository>();
builder.Services.AddSingleton<ApplicationStartup>();

WebApplication app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

ApplicationStartup startup = app.Services.GetRequiredService<ApplicationStartup>();
startup.RegisterApplicationEvents();

app.Run();
