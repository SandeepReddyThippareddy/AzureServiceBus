using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Text;

namespace ServiceBusQueueTrigger
{
    public class ProcessMessages
    {
        [FunctionName("ProcessMessages")]
        public void Run([ServiceBusTrigger("samplequeue", Connection = "servicebus-connection")]Message message, ILogger log)
        {
            log.LogInformation($"{Encoding.UTF8.GetString(message.Body)}");
            log.LogInformation($"{message.ContentType}");
            log.LogInformation($"{message.SessionId}");
            log.LogInformation($"{message.MessageId}");
        }
    }
}
