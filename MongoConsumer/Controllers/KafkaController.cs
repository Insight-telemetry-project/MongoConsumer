using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MongoConsumer.Services.Kafka;

namespace MongoConsumer.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class KafkaController : ControllerBase
    {
        private readonly KafkaConsumerService _consumerService;

        public KafkaController(KafkaConsumerService consumerService)
        {
            _consumerService = consumerService;
        }

        [HttpGet("test")]
        public async Task<IActionResult> TestConsume()
        {
            List<string> messages = await _consumerService.ConsumeNewMessagesAsync(5);

            if (messages.Count == 0)
            {
                return Ok("No new messages received from Kafka.");
            }
            return Ok(messages);
        }
    }
}
