using MongoConsumer.Services.Kafka;
using MongoConsumer.Services.Application;
using MongoConsumer.Models.Interface;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
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
