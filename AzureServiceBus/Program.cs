using Azure.Messaging.ServiceBus;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;

namespace AzureServiceBus
{
    internal class Program
    {
        static void Main(string[] args)
        {

            List<Order> orders = new List<Order>()
            {
                new Order()
                {
                    OrderID = "01",
                    Quantity = "1",
                    UnitPrice = "10"
                },
                new Order()
                {
                    OrderID = "02",
                    Quantity = "2",
                    UnitPrice = "20"
                },
                new Order()
                {
                    OrderID = "03",
                    Quantity = "3",
                    UnitPrice = "30"
                }
            };

            #region Queues

            var connectionStringQueue = "";
            var queueName = "samplequeue";


            ServiceBusClient serviceBusClientQueue = new ServiceBusClient(connectionStringQueue);

            #region Send Message
            //ServiceBusSender serviceBusSender = serviceBusClientQueue.CreateSender(queueName);

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ContentType = "application/json";

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();
            //}

            //Console.WriteLine("Messages Sent");

            #endregion

            #region Send Message with Time to Live set to 30 Seconds
            //ServiceBusSender serviceBusSender = serviceBusClientQueue.CreateSender(queueName);

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ContentType = "application/json";

            //    message.TimeToLive = TimeSpan.FromSeconds(30); ;

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();
            //}

            //Console.WriteLine("Messages Sent");

            #endregion

            #region PEEK Single Message
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientQueue.CreateReceiver(queueName);

            //ServiceBusReceivedMessage serviceBusReceivedMessage = serviceBusReceiver.PeekMessageAsync().GetAwaiter().GetResult();

            //Console.WriteLine(serviceBusReceivedMessage.Body);
            //Console.WriteLine(JsonSerializer.Deserialize<Order>(serviceBusReceivedMessage.Body));
            //Console.WriteLine(serviceBusReceivedMessage.ContentType);
            #endregion

            #region PEEK Multiple Messages
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientQueue.CreateReceiver(queueName);

            //IReadOnlyList<ServiceBusReceivedMessage> serviceBusReceivedMessages = serviceBusReceiver.PeekMessagesAsync(10).GetAwaiter().GetResult();


            //foreach (var message in serviceBusReceivedMessages)
            //{
            //    Console.WriteLine(message.Body);
            //    Console.WriteLine(JsonSerializer.Deserialize<Order>(message.Body));
            //    Console.WriteLine(message.ContentType);
            //}

            #endregion

            #region Receive Single Message - ReceiveMode = ServiceBusReceiveMode.PeekLock and Delete Message after processing
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientQueue.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

            //ServiceBusReceivedMessage serviceBusReceivedMessage = serviceBusReceiver.ReceiveMessageAsync().GetAwaiter().GetResult();

            ////Responsible for Deleting the Message from QUEUE
            //serviceBusReceiver.CompleteMessageAsync(serviceBusReceivedMessage).GetAwaiter().GetResult();

            //Console.WriteLine(serviceBusReceivedMessage.Body);
            //Console.WriteLine(JsonSerializer.Deserialize<Order>(serviceBusReceivedMessage.Body));
            //Console.WriteLine(serviceBusReceivedMessage.ContentType);
            #endregion

            #region Receive Multiple Messages - ReceiveMode = ServiceBusReceiveMode.PeekLock and Delete Messages after processing
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientQueue.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

            //IReadOnlyList<ServiceBusReceivedMessage> serviceBusReceivedMessages = serviceBusReceiver.ReceiveMessagesAsync(10).GetAwaiter().GetResult();


            //foreach (var message in serviceBusReceivedMessages)
            //{
            //    ////Responsible for Deleting the Message from QUEUE
            //    serviceBusReceiver.CompleteMessageAsync(message).GetAwaiter().GetResult();

            //    Console.WriteLine(message.Body);
            //    Console.WriteLine(JsonSerializer.Deserialize<Order>(message.Body));
            //    Console.WriteLine(message.ContentType);
            //}

            #endregion

            #region Receive Single Message - ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientQueue.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            //ServiceBusReceivedMessage serviceBusReceivedMessage = serviceBusReceiver.PeekMessageAsync().GetAwaiter().GetResult();

            //Console.WriteLine(serviceBusReceivedMessage.Body);
            //Console.WriteLine(JsonSerializer.Deserialize<Order>(serviceBusReceivedMessage.Body));
            //Console.WriteLine(serviceBusReceivedMessage.ContentType);
            #endregion

            #region Receive Multiple Messages - ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientQueue.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            //IReadOnlyList<ServiceBusReceivedMessage> serviceBusReceivedMessages = serviceBusReceiver.PeekMessagesAsync(10).GetAwaiter().GetResult();


            //foreach (var message in serviceBusReceivedMessages)
            //{
            //    Console.WriteLine(message.Body);
            //    Console.WriteLine(JsonSerializer.Deserialize<Order>(message.Body));
            //    Console.WriteLine(message.ContentType);
            //}

            #endregion

            #region Receive Single Message - ReceiveMode = ServiceBusReceiveMode.PeekLock and wait till Message Lock Duration before attempting to Delete Messages after processing
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientQueue.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

            //ServiceBusReceivedMessage serviceBusReceivedMessages = serviceBusReceiver.ReceiveMessageAsync().GetAwaiter().GetResult();


            ////Waiting for  Message Time To Live to hit
            //Thread.Sleep(TimeSpan.FromMinutes(2));

            //try
            //{
            //    //Responsible for Deleting the Message from QUEUE
            //    serviceBusReceiver.CompleteMessageAsync(serviceBusReceivedMessages).GetAwaiter().GetResult();
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine(ex.Message);
            //}

            //Console.WriteLine(serviceBusReceivedMessages.Body);
            //Console.WriteLine(JsonSerializer.Deserialize<Order>(serviceBusReceivedMessages.Body));
            //Console.WriteLine(serviceBusReceivedMessages.ContentType);

            #endregion

            #region Duplicate Message Detection
            //ServiceBusSender serviceBusSender = serviceBusClientQueue.CreateSender(queueName);

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ContentType = "application/json";

            //    //Used to Identify duplicate message - Service Bus will reject if found any
            //    message.MessageId = "1";

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();
            //}

            //Console.WriteLine("Messages Sent");
            #endregion

            #endregion


            #region Topics

            var connectionStringTopic = "";
            var topicName = "sampletopic";
            var topicSubscriptionName = "SubscriptionA";

            ServiceBusClient serviceBusClientTopic = new ServiceBusClient(connectionStringTopic);

            #region Send Messages to Topic
            //ServiceBusSender serviceBusSender = serviceBusClientTopic.CreateSender(topicName);

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ContentType = "application/json";

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();

            //}

            //Console.WriteLine("Messages Sent to Topic");

            #endregion

            #region Send Message to Topic With SQL Filter set to sys.messageid='3'
            //ServiceBusSender serviceBusSender = serviceBusClientTopic.CreateSender(topicName);

            //int count = 1;

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ContentType = "application/json";

            //    message.MessageId = count.ToString();

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();

            //    count = count + 1;

            //}

            //Console.WriteLine("Messages Sent to Topic");
            #endregion

            #region Send Message to Topic With SQL Filter set to name='sandeep'
            //ServiceBusSender serviceBusSender = serviceBusClientTopic.CreateSender(topicName);

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ContentType = "application/json";

            //    message.ApplicationProperties.Add("name", "sandeep");

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();

            //}

            //Console.WriteLine("Messages Sent to Topic");
            #endregion

            #region Send Message to Topic With Correlation Filter set to contentType='application/json'
            //ServiceBusSender serviceBusSender = serviceBusClientTopic.CreateSender(topicName);

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ContentType = "application/json";

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();
            //}

            //Console.WriteLine("Messages Sent to Topic");
            #endregion

            #region Send Message to Topic With Correlation Filter set to User-Defined Key-Value pairs : 'name' = 'sandeep'
            //ServiceBusSender serviceBusSender = serviceBusClientTopic.CreateSender(topicName);

            //foreach (var order in orders)
            //{
            //    ServiceBusMessage message = new ServiceBusMessage(order.ToString());

            //    message.ApplicationProperties.Add("name", "sandeep");

            //    serviceBusSender.SendMessageAsync(message).GetAwaiter().GetResult();
            //}

            //Console.WriteLine("Messages Sent to Topic");
            #endregion

            #region Receive Single Message from Topic as a Subscriber - ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientTopic.CreateReceiver(topicName, topicSubscriptionName, new ServiceBusReceiverOptions() {ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            //ServiceBusReceivedMessage serviceBusReceivedMessage = serviceBusReceiver.ReceiveMessageAsync().GetAwaiter().GetResult();

            //Console.WriteLine(serviceBusReceivedMessage.MessageId);
            //Console.WriteLine(serviceBusReceivedMessage.Body);
            //Console.WriteLine(serviceBusReceivedMessage.ContentType);
            //Console.WriteLine(serviceBusReceivedMessage.SequenceNumber);
            #endregion

            #region Receive Multiple Messages from Topic as a Subscriber - ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientTopic.CreateReceiver(topicName, topicSubscriptionName, new ServiceBusReceiverOptions() {ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            //IReadOnlyList<ServiceBusReceivedMessage> serviceBusReceivedMessages = serviceBusReceiver.ReceiveMessagesAsync(10).GetAwaiter().GetResult();


            //foreach (var message in serviceBusReceivedMessages)
            //{
            //    Console.WriteLine(message.MessageId);
            //    Console.WriteLine(message.Body);
            //    Console.WriteLine(message.ContentType);
            //    Console.WriteLine(message.SequenceNumber);
            //}
            #endregion

            #region Receive Single Message from Topic as a Subscriber - ReceiveMode = ServiceBusReceiveMode.PeekLock
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientTopic.CreateReceiver(topicName, topicSubscriptionName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

            //ServiceBusReceivedMessage serviceBusReceivedMessage = serviceBusReceiver.ReceiveMessageAsync().GetAwaiter().GetResult();

            ////This will delete the message from Subscription
            //serviceBusReceiver.CompleteMessageAsync(serviceBusReceivedMessage);

            //Console.WriteLine(serviceBusReceivedMessage.MessageId);
            //Console.WriteLine(serviceBusReceivedMessage.Body);
            //Console.WriteLine(serviceBusReceivedMessage.ContentType);
            //Console.WriteLine(serviceBusReceivedMessage.SequenceNumber);
            #endregion

            #region Receive Multiple Messages from Topic as a Subscriber - ReceiveMode = ServiceBusReceiveMode.PeekLock
            //ServiceBusReceiver serviceBusReceiver = serviceBusClientTopic.CreateReceiver(topicName, topicSubscriptionName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

            //IReadOnlyList<ServiceBusReceivedMessage> serviceBusReceivedMessages = serviceBusReceiver.ReceiveMessagesAsync(10).GetAwaiter().GetResult();


            //foreach (var message in serviceBusReceivedMessages)
            //{

            //    serviceBusReceiver.CompleteMessageAsync(message).Wait();

            //    Console.WriteLine(message.MessageId);
            //    Console.WriteLine(message.Body);
            //    Console.WriteLine(message.ContentType);
            //    Console.WriteLine(message.SequenceNumber);
            //}
            #endregion

            #endregion

            Console.ReadKey();
        }
    }
}
