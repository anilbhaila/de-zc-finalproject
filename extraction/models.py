import json
import dataclasses

from dataclasses import dataclass
from datetime import datetime

@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds


@dataclass
class TrafficEvent:
    ID: str
    RegionName: str
    Severity: str
    RoadwayName: str
    DirectionOfTravel: str
    Description: str
    Location: str
    LanesAffected: str
    LanesStatus: str
    PrimaryLocation: str
    SecondaryLocation: str
    FirstArticleCity: str
    SecondCity: str
    EventType: str
    EventSubType: str
    MapEncodedPolyline: str
    LastUpdated: str
    Latitude: str
    Longitude: str
    PlannedEndDate: str
    Reported: str
    StartDate: str
    ingestion_time: int
    processing_time: int

    def __init__(self, ID, RegionName, Severity, RoadwayName, DirectionOfTravel, Description, Location, LanesAffected, LanesStatus, PrimaryLocation, SecondaryLocation, FirstArticleCity, SecondCity, EventType, EventSubType, MapEncodedPolyline, LastUpdated, Latitude, Longitude, PlannedEndDate, Reported, StartDate, ingestion_time=None, processing_time=None):
        self.ID = ID
        self.RegionName = RegionName
        self.Severity = Severity
        self.RoadwayName = RoadwayName
        self.DirectionOfTravel = DirectionOfTravel
        self.Description = Description
        self.Location = Location
        self.LanesAffected = LanesAffected
        self.LanesStatus = LanesStatus
        self.PrimaryLocation = PrimaryLocation
        self.SecondaryLocation = SecondaryLocation
        self.FirstArticleCity = FirstArticleCity
        self.SecondCity = SecondCity
        self.EventType = EventType
        self.EventSubType = EventSubType
        self.MapEncodedPolyline = MapEncodedPolyline
        self.LastUpdated = LastUpdated
        self.Latitude = Latitude
        self.Longitude = Longitude
        self.PlannedEndDate = PlannedEndDate
        self.Reported = Reported
        self.StartDate = StartDate
        self.ingestion_time = ingestion_time if ingestion_time is not None else int(datetime.now().timestamp() * 1000)
        self.processing_time = processing_time if processing_time is not None else int(datetime.now().timestamp() * 1000)
    

def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

def event_serializer(event):
    event_dict = dataclasses.asdict(event)
    event_json = json.dumps(event_dict).encode('utf-8')
    return event_json

def event_deserializer(data):
    json_str = data.decode('utf-8')
    event_dict = json.loads(json_str)
    return TrafficEvent(**event_dict)

def event_from_row(row):
    return TrafficEvent(
        ID=row['ID'],
        RegionName=row['RegionName'],
        Severity=row['Severity'],
        RoadwayName=row['RoadwayName'],
        DirectionOfTravel=row['DirectionOfTravel'],
        Description=row['Description'],
        Location=row['Location'],
        LanesAffected=row['LanesAffected'],
        LanesStatus=row['LanesStatus'],
        PrimaryLocation=row['PrimaryLocation'],
        SecondaryLocation=row['SecondaryLocation'],
        FirstArticleCity=row['FirstArticleCity'],
        SecondCity=row['SecondCity'],
        EventType=row['EventType'],
        EventSubType=row['EventSubType'],
        MapEncodedPolyline=row['MapEncodedPolyline'],
        LastUpdated=row['LastUpdated'],
        Latitude=row['Latitude'],
        Longitude=row['Longitude'],
        PlannedEndDate=row['PlannedEndDate'],
        Reported=row['Reported'],
        StartDate=row['StartDate'],
        ingestion_time=int(datetime.now().timestamp() * 1000),
        processing_time=int(datetime.now().timestamp() * 1000)
    )