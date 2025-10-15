namespace MongoConsumer.Models.Configuration
{
    public class MongoSettings
    {
        public const string SectionName = "MongoSettings";

        public string ConnectionString { get; set; } = string.Empty;
        public string DatabaseName { get; set; } = string.Empty;
        public string CollectionName { get; set; } = string.Empty;
    }
}
