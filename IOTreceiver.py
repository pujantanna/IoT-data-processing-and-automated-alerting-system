import pyodbc, json
from datetime import datetime
from azure.eventhub import EventHubConsumerClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage

#EventHub Connection
connection_str = 'Endpoint=sb://ex6eventhubs.servicebus.windows.net/;SharedAccessKeyName=receiver;SharedAccessKey=2zrk7ophRvoXh3QZkej0S8EWfOUpXkjLI+AEhEEmGpo=;EntityPath=ex6eh'
eventhub_name = 'ex6eh'
consumer_group = '$Default'

#Queue Conncetion
CONNECTION_STR = 'Endpoint=sb://ex7servicebus.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=EhP0Xx1vsCIieyaSJdJg4I7nTx1Cbm5V1+ASbMg6bqo=;EntityPath=ex6queue'
QUEUE_NAME = 'ex6queue'

client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)
SQL_CONNECTION_STRING = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=test;UID=sa;PWD=Tanna009'

def process_event(event):
    conn = pyodbc.connect(SQL_CONNECTION_STRING)
    cursor = conn.cursor()

    try:
        cursor.execute("""EXEC ProcessDeviceEvent ?, ?, ?, ?""", event['DeviceID'], event['Temp'], event['Humidity'], datetime.fromisoformat(event['TimeStamp']))

        
        conn.commit()

    except pyodbc.ProgrammingError as e:
        print(f"ProgrammingError occurred: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
    finally:
        conn.close()

def on_event(partition_context, event):
    event_body = event.body_as_str(encoding='UTF-8')
    event_data = json.loads(event_body)
    process_event(event_data)
    print(f"Data Inserted: {event_data}")
    partition_context.update_checkpoint(event)

def send_message_to_queue(alert_data, alert_message, status):
    with ServiceBusClient.from_connection_string(CONNECTION_STR) as client:
        with client.get_queue_sender(QUEUE_NAME) as sender:
            message = ServiceBusMessage(json.dumps({"event": alert_data, "alert": alert_message, "status": status}))
            sender.send_messages(message)
            print(f"ALERT sent: {alert_data['DeviceID']} - {alert_message} - {status}")

try:
    with client:
        print("Listening for Events from the IOTDevices...")
        client.receive(on_event=on_event, starting_position="-1")
except KeyboardInterrupt:
    print("Stopping the receiver.")
finally:
    client.close()
