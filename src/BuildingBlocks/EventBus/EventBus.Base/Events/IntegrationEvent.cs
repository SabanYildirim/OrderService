using System;
using Newtonsoft.Json;

namespace EventBus.Base.Events
{
    public class IntegrationEvent
    {
        [JsonProperty]
        public Guid Id { get; private set; }
        
        [JsonProperty]
        public DateTime CreateDate { get; private set; }

        public IntegrationEvent()
        {
            Id = Guid.NewGuid();
            CreateDate = DateTime.Now;
        }
        
        [JsonConstructor]
        public IntegrationEvent(Guid id, DateTime createDate)
        {
            Id = id;
            CreateDate = createDate;    
        }
    }
}