using System;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using EventBus.Base;
using EventBus.Base.Abstraction;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventBus.AzureServiceBus
{
    public class EventBusServiceBus : BaseEventBus
    {
        private ITopicClient topicClient;
        private ManagementClient managementClient;
        private EventBusConfig EventBusConfig;
        private ILogger logger;
        
        public EventBusServiceBus(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
        {
            logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
            managementClient = new ManagementClient(config.EventBusConnectionString);
            topicClient = createTopicClient();
            EventBusConfig = config;
        }

        private ITopicClient createTopicClient()
        {
            if (topicClient == null || topicClient.IsClosedOrClosing)
            {
                topicClient = new TopicClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, RetryPolicy.Default);
            }

            if (!managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
                managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();

            return topicClient;
        }
        
        public override void Publish(IntegrationEvent @event)
        {
            var eventName = @event.GetType().Name; // example : OrderCreatedIntegrationEvent
            eventName = ProcessEventName(eventName); // example:  OrderCreated

            var eventStr = JsonConvert.SerializeObject(@event);
            var bodyStr = Encoding.UTF8.GetBytes(eventStr);
            
            var message = new Message()
            {
                MessageId = Guid.NewGuid().ToString(),
                Body = bodyStr,
                Label = eventName
            };
            topicClient.SendAsync(message).GetAwaiter().GetResult();
        }

        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);

            if (!subsManager.HasSubscriptionsForEvent(eventName))
            {
                var subscriptionClient = CreateSubscriptionClientIfNotExist(eventName);
                RegisterSubscriptionClientMessage(subscriptionClient);
            }
            
            logger.LogInformation("Subscribing to event {EventName}", eventName);
            subsManager.AddSubscription<T,TH>();
        }

        public override void UnSubscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            try
            {
                var subscriptionClient = CreateSubscriptionClient(eventName);
                
                subscriptionClient.RemoveRuleAsync(eventName)
                    .GetAwaiter()
                    .GetResult();
            }
            
            catch (MessagingEntityNotFoundException)
            {
                logger.LogWarning("The messaging entity {eventName} Could not be found.",eventName);
            }
            
            logger.LogInformation("Unsubscribing from event {EventName}", eventName);
            
            subsManager.RemoveSubscription<T,TH>();
        }

        private void RegisterSubscriptionClientMessage(ISubscriptionClient subscriptionClient)
        {
            subscriptionClient.RegisterMessageHandler(
                async (message, token) =>
                {
                    var eventName = $"{message.Label}";
                    var messageData = Encoding.UTF8.GetString(message.Body);

                    if (await ProcessEvent(ProcessEventName(eventName), messageData))
                    {
                        await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                    }
                },
                new MessageHandlerOptions(ExceptionReceivedHandler) { MaxConcurrentCalls = 10, AutoComplete = false});
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            var ex = exceptionReceivedEventArgs.Exception;
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            
            logger.LogError(ex,"ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}", ex.Message,context);
            return Task.CompletedTask;
        }
        
        private ISubscriptionClient CreateSubscriptionClientIfNotExist(string eventName)
        {
            var subClient = CreateSubscriptionClient(eventName);

            var exists = managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
            if (!exists)
            {
                managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
                RemoveDefaultRule(subClient);
            }
            CreateRuleIfNotExist(ProcessEventName(eventName),subClient);

            return subClient;
        }

        private void CreateRuleIfNotExist(string eventName, ISubscriptionClient subscriptionClient)
        {
            bool ruleExist;

            try
            {
                var rule = managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName)
                    .GetAwaiter().GetResult();

                ruleExist = rule != null;
            }
            catch (MessagingEntityNotFoundException)
            {
                //Azure Management Client doesn't have RuleExist method
                ruleExist = false;
            }

            if (!ruleExist)
            {
                subscriptionClient.AddRuleAsync(new RuleDescription
                {
                    Filter = new CorrelationFilter() { Label = eventName },
                    Name = eventName
                }).GetAwaiter().GetResult();
            }
        }
        
        private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
        {
            try
            {
                subscriptionClient
                    .RemoveRuleAsync(RuleDescription.DefaultRuleName)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (MessagingEntityNotFoundException)
            {
               logger.LogWarning("The messaging entity {DefaultRuleName} Could not be found",RuleDescription.DefaultRuleName);
            }
        }
        
        private SubscriptionClient CreateSubscriptionClient(string eventName)
        {
            return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
        }

        public override void Dispose()
        {
            base.Dispose();
            
            topicClient.CloseAsync().GetAwaiter().GetResult();
            topicClient = null;
            managementClient = null;
        }
    }
}