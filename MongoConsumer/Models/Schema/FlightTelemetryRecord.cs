using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;
using System.Text.Json.Serialization;

public class FlightTelemetryRecord
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string Id { get; set; } = string.Empty;

    [BsonElement("Fields")]
    [JsonPropertyName("Fields")]
    public Dictionary<string, double> Fields { get; set; } = new();

    [BsonElement("timestep")]
    [JsonPropertyName("timestep")]
    public int Timestep { get; set; }

    [BsonElement("Master Index")]
    [JsonPropertyName("Master Index")]
    public int MasterIndex { get; set; }
}
