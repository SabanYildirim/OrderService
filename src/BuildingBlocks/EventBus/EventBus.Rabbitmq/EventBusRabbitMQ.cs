using System;
using System.Collections;
using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EventBus.Rabbitmq
{
    public class EventBusRabbitMQ : BaseEventBus
    {
        private RabbitMQPersistentConnection rabbitMqPersistentConnection;
        private readonly IConnectionFactory connectionFactory;
        private readonly IModel consumerChannel;
        private readonly EventBusConfig _config;
        
        public EventBusRabbitMQ(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
        {
            _config = config;
            
            if (config.Connection != null)
            {
                var connJson = JsonConvert.SerializeObject(config.Connection, new JsonSerializerSettings()
                {
                    //Self refencing loop detected for property
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                });
                connectionFactory = JsonConvert.DeserializeObject<ConnectionFactory>(connJson);
            }
            else
                connectionFactory = new ConnectionFactory();

            rabbitMqPersistentConnection = new RabbitMQPersistentConnection(connectionFactory,config.ConnectionRetryCount);

            consumerChannel = CreateConsumerChannel();
        }

        public override void Publish(IntegrationEvent @event)
        {
            throw new NotImplementedException();
        }

        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);

            if (!subsManager.HasSubscriptionsForEvent(eventName))
            {
                if (!rabbitMqPersistentConnection.IsConnection)
                {
                    rabbitMqPersistentConnection.TryConnection();
                }

                consumerChannel.QueueDeclare(queue: GetSubName(eventName), // Ensure queue exist while consuming
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
                
                consumerChannel.QueueBind(queue : GetSubName(eventName),
                    exchange: _config.DefaultTopicName,
                    routingKey : eventName);
                StartBasicConsume(eventName);
            }
        }

        public override void UnSubscribe<T, TH>()
        {
            subsManager.RemoveSubscription<T,TH>();
        }

        private IModel CreateConsumerChannel()
        {
            if (!rabbitMqPersistentConnection.IsConnection)
            {
                rabbitMqPersistentConnection.TryConnection();
            }

            var channel = rabbitMqPersistentConnection.CreateModel();
            channel.ExchangeDeclare(exchange: _config.DefaultTopicName,
                type: "direct");

            return channel;
        }

        private void StartBasicConsume(string eventName)
        {
            if (consumerChannel != null)
            {
                var consumer = new EventingBasicConsumer(consumerChannel);

                consumer.Received += Consumer_Received;

                consumerChannel.BasicConsume(
                    queue: GetSubName(eventName),
                    autoAck: false,
                    consumer : consumer);
            }
        }

        private async void Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var eventName = eventArgs.RoutingKey;
            eventName = ProcessEventName(eventName);
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

            try
            {
                await ProcessEvent(eventName, message);
            }
            catch (Exception ex)
            {
                // logging
            }
            
            consumerChannel.BasicAck(eventArgs.DeliveryTag,multiple:false);
        }
    }
}