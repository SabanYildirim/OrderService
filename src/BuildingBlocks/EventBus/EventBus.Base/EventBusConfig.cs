using System;
using System.Diagnostics.SymbolStore;

namespace EventBus.Base
{
    public class EventBusConfig
    {
        public int ConnectionRetryCount { get; set; } = 5;

        public string DefaultTopicName { get; set; } = "SellingBuddyName";
        
        public string EventBusConnectionString { get; set; } = String.Empty;

        public string SubscriberClientAppName { get; set; } = string.Empty;

        public string EventNamePrefix { get; set; } = string.Empty;

        public string EventNameSuffix { get; set; } = "IntegrationEvent";

        public EventBusType EventBusType { get; set; } = EventBusType.RabbitMQ;
        
        public object Connection { get; set; }

        public bool DeleteEventPrefix => !string.IsNullOrEmpty(EventNamePrefix);
        public bool DeleteEventSuffix => !string.IsNullOrEmpty(EventNameSuffix);
    }

    public enum EventBusType
    {
        RabbitMQ = 0,
        AzureServiceBus = 1
    }
}