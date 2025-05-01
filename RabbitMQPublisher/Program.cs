
using RabbitMQ.Client;
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

Console.Read();