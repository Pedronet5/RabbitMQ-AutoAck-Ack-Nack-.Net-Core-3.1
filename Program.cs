using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            var manualResetEvent = new ManualResetEvent(false);

            manualResetEvent.Reset();

            using (var connection = factory.CreateConnection())
            {
                var queueName = "order";

                var channel1 = CreateChannel(connection, queueName);
                BuildAndRunPublishers(channel1, queueName, "Produtor A", manualResetEvent);

                manualResetEvent.WaitOne();
            }
        }

        public static IModel CreateChannel(IConnection connection, string queueName)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            return channel;
        }

        public static void BuildAndRunPublishers(IModel channel, string queue, string publisherName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                while (true)
                {
                    int count = 0;

                    try
                    {
                        Console.WriteLine("Pressione qualquer tecla para produzir 1000 msg");
                        Console.ReadLine();

                        for (int index = 0; index < 1000; index++)
                        {
                            var message = $"OrderNumber: {count++} from {publisherName}";
                            var body = Encoding.UTF8.GetBytes(message);

                            channel.BasicPublish("", queue, null, body);

                            Console.WriteLine($"{publisherName} - [x] Sent {count}", message);
                        }
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine(ex.Message);

                        manualResetEvent.Set();
                    }
                }
            });
        }
    }
}
