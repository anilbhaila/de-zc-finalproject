import psycopg2

from kafka import KafkaConsumer
from datetime import datetime
from tkinter import ON

from models import event_deserializer

server = 'localhost:9092'
topic_name = 'ny-traffic-events'


consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='traffic-events-database',
    value_deserializer=event_deserializer
)


conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='ny_traffic',
    user='root',
    password='root'
)

conn.autocommit = True
cur = conn.cursor()


print(f"Listening to {topic_name} and writing to PostgreSQL...")

cur.execute('''
    CREATE TABLE IF NOT EXISTS processed_traffic_events (
        ID VARCHAR(255) PRIMARY KEY,
        RegionName VARCHAR(255),
        Severity VARCHAR(255),
        RoadwayName VARCHAR(255),
        DirectionOfTravel VARCHAR(255),
        Description TEXT,
        Location VARCHAR(255),
        LanesAffected VARCHAR(255),
        LanesStatus VARCHAR(255),
        PrimaryLocation VARCHAR(255),
        SecondaryLocation VARCHAR(255),
        FirstArticleCity VARCHAR(255),
        SecondCity VARCHAR(255),
        EventType VARCHAR(255),
        EventSubType VARCHAR(255),
        MapEncodedPolyline TEXT,
        LastUpdated VARCHAR(255),
        Latitude VARCHAR(255),
        Longitude VARCHAR(255),
        PlannedEndDate VARCHAR(255),
        Reported VARCHAR(255),
        StartDate VARCHAR(255),
        ingestion_time TIMESTAMP,
        processing_time TIMESTAMP
    )
''')

print("Table created or already exists. Starting to consume messages...")

count = 0
for message in consumer:
    # print(f"Received message: {message.value}")
    trafficEvent = message.value
    cur.execute(
        """INSERT INTO processed_traffic_events
           (ID, RegionName, Severity, RoadwayName, DirectionOfTravel, Description, Location, LanesAffected, LanesStatus, PrimaryLocation, SecondaryLocation, FirstArticleCity, SecondCity, EventType, EventSubType, MapEncodedPolyline, LastUpdated, Latitude, Longitude, PlannedEndDate, Reported, StartDate, ingestion_time, processing_time)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s))
            ON CONFLICT (ID) 
            DO UPDATE SET
            RegionName = EXCLUDED.RegionName,
            Severity = EXCLUDED.Severity,
            RoadwayName = EXCLUDED.RoadwayName,
            DirectionOfTravel = EXCLUDED.DirectionOfTravel,
            Description = EXCLUDED.Description,
            Location = EXCLUDED.Location,
            LanesAffected = EXCLUDED.LanesAffected,
            LanesStatus = EXCLUDED.LanesStatus,
            PrimaryLocation = EXCLUDED.PrimaryLocation,
            SecondaryLocation = EXCLUDED.SecondaryLocation,
            FirstArticleCity = EXCLUDED.FirstArticleCity,
            SecondCity = EXCLUDED.SecondCity,
            EventType = EXCLUDED.EventType,
            EventSubType = EXCLUDED.EventSubType,
            MapEncodedPolyline = EXCLUDED.MapEncodedPolyline,
            LastUpdated = EXCLUDED.LastUpdated,
            Latitude = EXCLUDED.Latitude,
            Longitude = EXCLUDED.Longitude,
            PlannedEndDate = EXCLUDED.PlannedEndDate,
            Reported = EXCLUDED.Reported,
            StartDate = EXCLUDED.StartDate,
            processing_time = EXCLUDED.processing_time  
        """,
        (trafficEvent.ID, 
         trafficEvent.RegionName,
         trafficEvent.Severity,
         trafficEvent.RoadwayName,
         trafficEvent.DirectionOfTravel,
         trafficEvent.Description,
         trafficEvent.Location,
         trafficEvent.LanesAffected,
         trafficEvent.LanesStatus,
         trafficEvent.PrimaryLocation,
         trafficEvent.SecondaryLocation,
         trafficEvent.FirstArticleCity,
         trafficEvent.SecondCity,
         trafficEvent.EventType,
         trafficEvent.EventSubType,
         trafficEvent.MapEncodedPolyline,
         trafficEvent.LastUpdated,
         trafficEvent.Latitude,
         trafficEvent.Longitude,
         trafficEvent.PlannedEndDate,
         trafficEvent.Reported,
         trafficEvent.StartDate,
         trafficEvent.ingestion_time,
         trafficEvent.processing_time)
    )
    count += 1
    if count % 100 == 0:
        print(f"Inserted {count} rows...")

consumer.close()
cur.close()
conn.close()