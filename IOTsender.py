import time, pytz, json, random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

connection_str = 'Endpoint=sb://ex6eventhubs.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=PJahQpDinMOjlhk6tndJOi/Z4H0V6ekJK+AEhO6Z354=;EntityPath=ex6eh'
eventhub_name = 'ex6eh'
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)
ist = pytz.timezone('Asia/Kolkata')

def generate_event():  
 return {
            'DeviceID': random.choice([6300,9900]) ,
            'Temp': "{:.2f}".format(random.uniform(10, 45)),
            'Humidity': "{:.2f}".format(random.uniform(35, 95)),
            'TimeStamp': datetime.now(ist).isoformat()
        }

try:
    while True:
         event = generate_event()
         event_data = EventData(json.dumps(event))
         producer.send_event(event_data)
         print(f"Sent event: {event}")
         time.sleep(60)

except KeyboardInterrupt:
    print("Stopping the sender.")
finally:
    producer.close()
