import os
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random


LONDON_COORDINATES = {"latitude": 51.5074, "longitude" : -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}


# Calculate movt increment
LATITUDE_INCREMENT = ( BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude'])/100

LONGITUDE_INCREMENT = ( BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude'])/100

KAFKA_BOOTSTARAP_SERVERS = os.getenv('KAFKA_BOOTSTARAP_SERVER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPICS','gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'vehicle_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergerncy_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds = random.randint(30, 60))
    return start_time



def generate_gps_data(device_id, timestamp, vehicle_type = 'private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
        
    }
    
def generate_traffic_camera_data(device_id, timestamp,location, camera_id):
    return{
        'id': uuid.uuid4(),
        'device': device_id,
        'cameraId': camera_id,
        'loation': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EnodedString'
    }
    
def generate_wheather_data( device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform( -5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0,100),
        'humidiyt': random.randint(0, 100),
        'airQualityIndex': random.uniform( 0, 500),     
        
    }



def simulate_vehicle_movement():
    global start_location
    
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    
    
    # add randomness to simulate actual travel
    start_location['latitude'] += random.uniform( -0.0005, 0.0005) #LATITUDE_INCREMENT
    start_location['longitude'] += random.uniform( -0.0005, 0.0005) #LONGITUDE_INCREMENT
    



def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitiude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East', 
        'make': 'BMW',
        'model': 'C500',
        'year': '2024',
        'fuleType': 'Hybrid'
        
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Descritop of the incident'
    
        
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of tpe {bj.__class__.__name__.} is not JSON serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed:  {err}')

    else:
        print(f'Message delivered to {msg.topic()_} [{ms.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic, 
        key=str(data['id']), 
        value= json.dumps(data,default=json_serializer).endocde('utf-8'),
        on_delivery = delivery_report
    )

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id= 'NIkon-Cam123')
        whether_data = generate_wheather_data( device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(whether_data)
        # print(emergency_incident_data)
        
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffi)
        produce_data_to_kafka(producer, WEATHER_TOPIC, whether_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        
        break



if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTARAP_SERVERS,
        'error_cb': lambda err: print(f'kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)
    
    try: 
        simulate_journey(producer, 'Vehicle-CodeWithYu-123')
    except KeyboardInterrupt:
        print("Simulation ended by the user")
    except Exception as e:
    except Exception as e:
        print(f' Unexpected error {e}')
    




