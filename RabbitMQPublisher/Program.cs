
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

//Bağlantı Oluşturma
ConnectionFactory factory = new();
factory.Uri = new("amqps://mbcrvfxw:gup02iQVWWbWQpURp6_gUaysguCCgUbi@rattlesnake.rmq.cloudamqp.com/mbcrvfxw");

//Bağlantıyı Aktifleştirme ve Kanal Açma

using IConnection connection = factory.CreateConnection();
using IModel channel = connection.CreateModel();

//Queue Oluşturma
channel.QueueDeclare(queue: "example-queue", exclusive: false);

//Queue'ya Mesaj Gönderme (RabbitMq kuyruğa atacağı mesajları byte türünden kabul etmektedir.Haliyle mesajları bizim byte'a dönüştürmemiz gerekecektir.

byte[] message = Encoding.UTF8.GetBytes("Merhaba");
channel.BasicPublish(exchange: "", routingKey: "example-queue", body: message);

#region DirectExchange

//1. Adım
channel.ExchangeDeclare(exchange: "direct-exchange-example", type: ExchangeType.Direct);

//2. Adım
string queueName = channel.QueueDeclare().QueueName;

//3. Adım
channel.QueueBind(
    queue: queueName,
    exchange: "direct-exchange-example",
    routingKey: "direct-queue-example");

EventingBasicConsumer consumer = new(channel);
channel.BasicConsume(
    queue: queueName,
    autoAck: true,
    consumer: consumer);
consumer.Received += (sender, e) =>
{
    string message = Encoding.UTF8.GetString(e.Body.Span);
    Console.WriteLine(message);
};
#endregion

#region FanoutExchange
channel.ExchangeDeclare(
    exchange: "fanout-exchange-example",
    type: ExchangeType.Fanout);

for (int i = 0; i < 100; i++)
{
    await Task.Delay(200);
    byte[] message2 = Encoding.UTF8.GetBytes($"Merhaba {i}");

    channel.BasicPublish(
        exchange: "fanout-exchange-example",
        routingKey: string.Empty,
        body: message2);
}
#endregion

#region TopicExchange
channel.ExchangeDeclare(
    exchange: "topic-exchange-example",
    type: ExchangeType.Topic
    );

for (int i = 0; i < 100; i++)
{
    await Task.Delay(200);
    byte[] message3 = Encoding.UTF8.GetBytes($"Merhaba {i}");
    Console.Write("Mesajın gönderileceği topic formatını belirtiniz : ");
    string topic = Console.ReadLine();
    channel.BasicPublish(
        exchange: "topic-exchange-example",
        routingKey: topic,
        body: message3
        );
}

#endregion

#region HeaderExchange
//channel.ExchangeDeclare(
//    exchange: "header-exchange-example",
//    type: ExchangeType.Headers);

//for (int i = 0; i < 100; i++)
//{
//    await Task.Delay(200);
//    byte[] message4 = Encoding.UTF8.GetBytes($"Merhaba {i}");
//    Console.Write("Lütfen header value'sunu giriniz : ");
//    string value = Console.ReadLine();

//    IBasicProperties basicProperties = channel.CreateBasicProperties();
//    basicProperties.Headers = new Dictionary<string, object>
//    {
//        ["no"] = value
//    };

//    channel.BasicPublish(
//        exchange: "header-exchange-example",
//        routingKey: string.Empty,
//        body: message4,
//        basicProperties: basicProperties
//        );
//}
#endregion



#region P2P (Point-to-Point) Tasarımı
//string queueName = "example-p2p-queue";

//channel.QueueDeclare(
//    queue: queueName,
//    durable: false,
//    exclusive: false,
//    autoDelete: false);

//byte[] message = Encoding.UTF8.GetBytes("merhaba");
//channel.BasicPublish(
//    exchange: string.Empty,
//    routingKey: queueName,
//    body: message);
#endregion
#region Publish/Subscribe (Pub/Sub) Tasarımı
//string exchangeName = "example-pub-sub-exchange";

//channel.ExchangeDeclare(
//    exchange: exchangeName,
//    type: ExchangeType.Fanout);

//for (int i = 0; i < 100; i++)
//{
//    await Task.Delay(200);

//    byte[] message = Encoding.UTF8.GetBytes("merhaba" + i);

//    channel.BasicPublish(
//        exchange: exchangeName,
//        routingKey: string.Empty,
//        body: message);
//}

#endregion
#region Work Queue(İş Kuyruğu) Tasarımı​
//string queueName = "example-work-queue";

//channel.QueueDeclare(
//    queue: queueName,
//    durable: false,
//    exclusive: false,
//    autoDelete: false);

//for (int i = 0; i < 100; i++)
//{
//    await Task.Delay(200);

//    byte[] message = Encoding.UTF8.GetBytes("merhaba" + i);

//    channel.BasicPublish(
//        exchange: string.Empty,
//        routingKey: queueName,
//        body: message);
//}

#endregion
#region Request/Response Tasarımı​
//string requestQueueName = "example-request-response-queue";
//channel.QueueDeclare(
//    queue: requestQueueName,
//    durable: false,
//    exclusive: false,
//    autoDelete: false);

//string replyQueueName = channel.QueueDeclare().QueueName;

//string correlationId = Guid.NewGuid().ToString();

#region Request Mesajını Oluşturma ve Gönderme
//IBasicProperties properties = channel.CreateBasicProperties();
//properties.CorrelationId = correlationId;
//properties.ReplyTo = replyQueueName;

//for (int i = 0; i < 10; i++)
//{
//    byte[] message = Encoding.UTF8.GetBytes("merhaba" + i);
//    channel.BasicPublish(
//        exchange: string.Empty,
//        routingKey: requestQueueName,
//        body: message,
//        basicProperties: properties);
//}
#endregion
#region Response Kuyruğu Dinleme
//EventingBasicConsumer consumer = new(channel);
//channel.BasicConsume(
//    queue: replyQueueName,
//    autoAck: true,
//    consumer: consumer);

//consumer.Received += (sender, e) =>
//{
//    if (e.BasicProperties.CorrelationId == correlationId)
//    {
//        //....
//        Console.WriteLine($"Response : {Encoding.UTF8.GetString(e.Body.Span)}");
//    }
//};
#endregion

#endregion


Console.Read();