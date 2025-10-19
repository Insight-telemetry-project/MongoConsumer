using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

public class FlightTelemetryRecord
{
    [BsonElement("Fields")]
    public Dictionary<string, double> Fields { get; set; } = new();

    [BsonElement("timestep")]
    public int Timestep { get; set; }

    [BsonElement("Master Index")]
    public int MasterIndex { get; set; }
}
