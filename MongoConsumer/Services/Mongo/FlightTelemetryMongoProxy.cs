using Microsoft.Extensions.Options;
using MongoConsumer.Models.Configuration;
using MongoConsumer.Models.Dto;
using MongoConsumer.Models.Interface;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

public class FlightTelemetryMongoProxy : ITelemetryRepository
{
    private IMongoCollection<FlightTelemetryRecord>? telemetryCollection;
    private readonly MongoSettings _mongoSettings;
    private bool _isInitialized = false;

    public FlightTelemetryMongoProxy(IOptions<MongoSettings> mongoOptions)
    {
        _mongoSettings = mongoOptions.Value;
    }

    public async Task InsertFlightTelemetryAsync(string jsonData)
    {
        if (!_isInitialized)
            await InitializeMongoAsync();

        FlightTelemetryFieldsDto dto = JsonSerializer.Deserialize<FlightTelemetryFieldsDto>(jsonData);


        dto.Fields.TryGetValue("timestep", out double timestepValue);
        dto.Fields.TryGetValue("Master Index", out double masterIndexValue);

        FlightTelemetryRecord record = new FlightTelemetryRecord
        {
            Timestep = (int)timestepValue,
            MasterIndex = (int)masterIndexValue,
            Fields = new Dictionary<string, double>(dto.Fields)
        };

        record.Fields.Remove("timestep");
        record.Fields.Remove("Master Index");

        await telemetryCollection.InsertOneAsync(record);
    }



    private async Task InitializeMongoAsync()
    {
        MongoClient mongoClient = new MongoClient(_mongoSettings.ConnectionString);
        IMongoDatabase mongoDatabase = mongoClient.GetDatabase(_mongoSettings.DatabaseName);

        using (IAsyncCursor<string> cursor = await mongoDatabase.ListCollectionNamesAsync())
        {
            List<string> collections = await cursor.ToListAsync();
            bool collectionExists = collections.Contains(_mongoSettings.CollectionName);

            if (!collectionExists)
                await mongoDatabase.CreateCollectionAsync(_mongoSettings.CollectionName);
        }

        telemetryCollection = mongoDatabase.GetCollection<FlightTelemetryRecord>(_mongoSettings.CollectionName);
        _isInitialized = true;
    }
}
