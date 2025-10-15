using Microsoft.Extensions.Options;
using MongoConsumer.Models.Configuration;
using MongoConsumer.Models.Interface;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

public class TelemetryRepository : ITelemetryRepository
{
    private readonly IMongoCollection<TelemetryRecord> telemetryCollection;

    public TelemetryRepository(IOptions<MongoSettings> mongoOptions)
    {
        MongoSettings mongoSettings = mongoOptions.Value;

        MongoClient mongoClient = new MongoClient(mongoSettings.ConnectionString);
        IMongoDatabase mongoDatabase = mongoClient.GetDatabase(mongoSettings.DatabaseName);

        bool collectionExists = mongoDatabase
            .ListCollectionNames()
            .ToList()
            .Contains(mongoSettings.CollectionName);

        if (!collectionExists)
        {
            mongoDatabase.CreateCollection(mongoSettings.CollectionName);
        }

        telemetryCollection = mongoDatabase.GetCollection<TelemetryRecord>(mongoSettings.CollectionName);
    }

    public async Task InsertJsonAsync(string jsonData)
    {
        JsonDocument jsonDocument = JsonDocument.Parse(jsonData);
        JsonElement fieldsElement = jsonDocument.RootElement.GetProperty("Fields");


        Dictionary<string, double> fields = new Dictionary<string, double>();
        int timestepValue = 0;
        int masterIndexValue = 0;

        foreach (JsonProperty property in fieldsElement.EnumerateObject())
        {
            string name = property.Name;
            double value = property.Value.GetDouble();

            if (name == "timestep")
            {
                timestepValue = (int)value;
            }
            else if (name == "Master Index")
            {
                masterIndexValue = (int)value;
            }
            else
            {
                fields[name] = value;
            }
        }

        TelemetryRecord telemetryRecord = new TelemetryRecord
        {
            Timestep = timestepValue,
            MasterIndex = masterIndexValue,
            Fields = fields
        };

        await telemetryCollection.InsertOneAsync(telemetryRecord);
    }
}
