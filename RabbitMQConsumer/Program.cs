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
channel.BasicConsume(queue:"example-queue",false,consumer);
consumer.Received += (sender, e) =>
{
    //Kuyruğa gelen mesajın işlendiği yerdir!
    //e.Body: Kuyruktaki mesajın verisini bütünsel olarak getirecektir.
    //e.Body.Span veya e.Body.ToArray() : Kuyruğundaki mesajın byte verisini getirecektir.

    Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
};
Console.Read();