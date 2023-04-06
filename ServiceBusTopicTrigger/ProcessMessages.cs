using System.Text;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace ServiceBusTopicTrigger
{
    public class ProcessMessages
    {
        private readonly ILogger<ProcessMessages> _logger;

        public ProcessMessages(ILogger<ProcessMessages> log)
        {
            _logger = log;
        }

        [FunctionName("ProcessMessages")]
        public void Run([ServiceBusTrigger("sampleTopic", "SubscriptionA", Connection = "servicebus-connection")]Message message)
        {
            _logger.LogInformation($"{Encoding.UTF8.GetString(message.Body)}");
            _logger.LogInformation($"{message.ContentType}");
            _logger.LogInformation($"{message.SessionId}");
            _logger.LogInformation($"{message.MessageId}");
        }
    }
}
