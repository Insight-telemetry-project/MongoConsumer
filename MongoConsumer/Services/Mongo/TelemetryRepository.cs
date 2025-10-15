using Microsoft.Extensions.Options;
using MongoConsumer.Models.Configuration;
using MongoConsumer.Models.Interface;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics;

namespace MongoConsumer.Services.Mongo
{
    public class TelemetryRepository : ITelemetryRepository
    {
        private readonly IMongoCollection<BsonDocument> _collection;

        public TelemetryRepository(IOptions<MongoSettings> mongoOptions)
        {
            MongoSettings settings = mongoOptions.Value;

            MongoClient client = new MongoClient(settings.ConnectionString);
            IMongoDatabase database = client.GetDatabase(settings.DatabaseName);
            _collection = database.GetCollection<BsonDocument>(settings.CollectionName);

            Debug.WriteLine($"Connected to MongoDB: {settings.DatabaseName}/{settings.CollectionName}");
        }

        public async Task InsertJsonAsync(string json)
        {
            try
            {
                BsonDocument document = BsonDocument.Parse(json);
                await _collection.InsertOneAsync(document);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error inserting document: {ex.Message}");
            }
        }
    }
}
