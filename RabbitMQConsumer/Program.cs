using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;


//Bağlantı Oluşturma
ConnectionFactory factory = new();
factory.Uri = new("amqps://mbcrvfxw:gup02iQVWWbWQpURp6_gUaysguCCgUbi@rattlesnake.rmq.cloudamqp.com/mbcrvfxw");

//Bağlantı Aktifleştirme
using IConnection connection = factory.CreateConnection();
using IModel channel=connection.CreateModel();


//Queue Oluşturma (Consumer'da da kuyruk publisher'daki ile birebir aynı yapılandırmada tanımlanmalıdır!!
channel.QueueDeclare(queue: "example-queue", exclusive: false);

//Queue'dan Mesaj okuma
EventingBasicConsumer consumer=new(channel);
channel.BasicConsume(queue:"example-queue",autoAck: false,consumer);
consumer.Received += (sender, e) =>
{
    //Kuyruğa gelen mesajın işlendiği yerdir!
    //e.Body: Kuyruktaki mesajın verisini bütünsel olarak getirecektir.
    //e.Body.Span veya e.Body.ToArray() : Kuyruğundaki mesajın byte verisini getirecektir.

    Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));

    channel.BasicAck(deliveryTag:e.DeliveryTag,multiple:false);
};

#region DirectExchange
channel.ExchangeDeclare(exchange: "direct-exchange-example", type: ExchangeType.Direct);

while (true)
{
    Console.Write("Mesaj : ");
    string message = Console.ReadLine();
    byte[] byteMessage = Encoding.UTF8.GetBytes(message);

    channel.BasicPublish(
        exchange: "direct-exchange-example",
        routingKey: "direct-queue-example",
        body: byteMessage);
}
#endregion

#region FanoutExchange
channel.ExchangeDeclare(
    exchange: "fanout-exchange-example",
    type: ExchangeType.Fanout);

Console.Write("Kuyruk adını giriniz : ");
string queueName = Console.ReadLine();

channel.QueueDeclare(
    queue: queueName,
    exclusive: false);

channel.QueueBind(
    queue: queueName,
    exchange: "fanout-exchange-example",
    routingKey: string.Empty
    );

EventingBasicConsumer consumer2 = new(channel);
channel.BasicConsume(
    queue: queueName,
    autoAck: true,
    consumer: consumer2);
consumer2.Received += (sender, e) =>
{
    string message = Encoding.UTF8.GetString(e.Body.Span);
    Console.WriteLine(message);
};
#endregion

#region TopicExchange
channel.ExchangeDeclare(
  exchange: "topic-exchange-example",
  type: ExchangeType.Topic
    );

Console.Write("Dinlenecek topic formatını belirtiniz : ");
string topic = Console.ReadLine();
string queueName3 = channel.QueueDeclare().QueueName;
channel.QueueBind(
    queue: queueName,
    exchange: "topic-exchange-example",
    routingKey: topic
    );

EventingBasicConsumer consumer3 = new(channel);
channel.BasicConsume(
    queue: queueName,
    autoAck: true,
    consumer3
    );

consumer3.Received += (sender, e) =>
{
    string message = Encoding.UTF8.GetString(e.Body.Span);
    Console.WriteLine(message);
};
#endregion

#region HeaderExchange
channel.ExchangeDeclare(
    exchange: "header-exchange-example",
    type: ExchangeType.Headers
    );

Console.Write("Lütfen header value'sunu giriniz : ");
string value = Console.ReadLine();

string queueName4 = channel.QueueDeclare().QueueName;

channel.QueueBind(
    queue: queueName4,
    exchange: "header-exchange-example",
    routingKey: string.Empty,
    new Dictionary<string, object>
    {
        ["no"] = value
    });

EventingBasicConsumer consumer4 = new(channel);
channel.BasicConsume(
    queue: queueName4,
    autoAck: true,
    consumer: consumer4
    );

consumer4.Received += (sender, e) =>
{
    string message = Encoding.UTF8.GetString(e.Body.Span);
    Console.WriteLine(message);
};
#endregion


#region P2P (Point-to-Point) Tasarımı
//string queueName = "example-p2p-queue";

//channel.QueueDeclare(
//    queue: queueName,
//    durable: false,
//    exclusive: false,
//    autoDelete: false);

//EventingBasicConsumer consumer = new(channel);
//channel.BasicConsume(
//    queue: queueName,
//    autoAck: false,
//    consumer: consumer);

//consumer.Received += (sender, e) =>
//{
//    Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
//};
#endregion
#region Publish/Subscribe (Pub/Sub) Tasarımı
//string exchangeName = "example-pub-sub-exchange";

//channel.ExchangeDeclare(
//    exchange: exchangeName,
//    type: ExchangeType.Fanout);

//string queueName = channel.QueueDeclare().QueueName;
//channel.QueueBind(
//    queue: queueName,
//    exchange: exchangeName,
//    routingKey: string.Empty);

//EventingBasicConsumer consumer = new(channel);
//channel.BasicConsume(
//    queue: queueName,
//    autoAck: false,
//    consumer: consumer);

//consumer.Received += (sender, e) =>
//{
//    Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
//};


#endregion
#region Work Queue(İş Kuyruğu) Tasarımı​
//string queueName = "example-work-queue";

//channel.QueueDeclare(
//    queue: queueName,
//    durable: false,
//    exclusive: false,
//    autoDelete: false);

//EventingBasicConsumer consumer = new(channel);
//channel.BasicConsume(
//    queue: queueName,
//    autoAck: true,
//    consumer: consumer);

//channel.BasicQos(
//    prefetchCount: 1,
//    prefetchSize: 0,
//    global: false);

//consumer.Received += (sender, e) =>
//{
//    Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
//};
#endregion
#region Request/Response Tasarımı​

//string requestQueueName = "example-request-response-queue";
//channel.QueueDeclare(
//    queue: requestQueueName,
//    durable: false,
//    exclusive: false,
//    autoDelete: false);

//EventingBasicConsumer consumer = new(channel);
//channel.BasicConsume(
//    queue: requestQueueName,
//    autoAck: true,
//    consumer: consumer);

//consumer.Received += (sender, e) =>
//{
//    string message = Encoding.UTF8.GetString(e.Body.Span);
//    Console.WriteLine(message);
//    //.....
//    byte[] responseMessage = Encoding.UTF8.GetBytes($"İşlem tamamlandı. : {message}");
//    IBasicProperties properties = channel.CreateBasicProperties();
//    properties.CorrelationId = e.BasicProperties.CorrelationId;
//    channel.BasicPublish(
//        exchange: string.Empty,
//        routingKey: e.BasicProperties.ReplyTo,
//        basicProperties: properties,
//        body: responseMessage);
//};

#endregion

Console.Read();