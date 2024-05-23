from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas
import json

def get_sensor(mongodb: Session, sensor_id: int) -> Optional[models.Sensor]:
    # Get sensor data on MongoDB by its id
    sensor_data = mongodb.get_sensor(sensor_id)
    if sensor_data:
        # Prepare data to be returned
        sensor_data["latitude"] = float(sensor_data["location"]["coordinates"][0])
        sensor_data["longitude"] = float(sensor_data["location"]["coordinates"][1])
        sensor_data.pop("location")
        return json.dumps(sensor_data)
    else:
        return

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, mongodb: Session, elastic: Session, cassandra: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    # Add data to Postgress
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    #  Add data to MongoDB
    data = {"id": db_sensor.id,
            "name": sensor.name,
            "type": sensor.type,
            "mac_address": sensor.mac_address,
            "manufacturer": sensor.manufacturer,
            "model": sensor.model,
            "serie_number": sensor.serie_number,
            "firmware_version": sensor.firmware_version,
            "description": sensor.description,
            "location": {"type": "Point", "coordinates": [sensor.latitude, sensor.longitude]}}
    mongodb.add_sensor(data)
    # Add data to ElasticSearch
    # Define the mapping for the index
    es_data = {
        "name": sensor.name,
        "type": sensor.type,
        "description": sensor.description
    }
    elastic.index_document('sensors', es_data)

    # Add 1 to Cassandra sensor type counter
    cassandra.execute(f"""
        UPDATE sensor.quantity 
        SET quantity = quantity+1  WHERE  type_sensor = '{sensor.type}';""")

    # Prepare data to be returned
    sensor = sensor.dict()
    sensor['id'] = db_sensor.id
    return sensor

def record_data(redis: Session, ts: Session, cassandra: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    # First we will add the data to TimeScale
    # We will prepare the query with placeholders
    query = f'''INSERT INTO sensor_data(id, battery_level, last_seen, temperature, humidity, velocity) 
             VALUES ({sensor_id}, {data.battery_level}, 
                     {f"'{data.last_seen}'" if data.last_seen is not None else 'NULL'}, 
                     {data.temperature if data.temperature is not None else 'NULL'}, 
                     {data.humidity if data.humidity is not None else 'NULL'}, 
                     {data.velocity if data.velocity is not None else 'NULL'})
             ON CONFLICT (id, last_seen) DO UPDATE SET temperature = EXCLUDED.temperature,
             humidity = EXCLUDED.humidity, velocity = EXCLUDED.velocity, battery_level = EXCLUDED.battery_level;'''
    # We will execute the query and update the database
    ts.execute(query)
    ts.execute('commit')

    # We will update Cassandra's tables as well
    if data.temperature is not None:
        cassandra.execute(f"""
            INSERT INTO sensor.temperature (id, last_seen, temperature)
            VALUES ({sensor_id}, toTimeStamp(now()), {data.temperature})""")

    cassandra.execute(f"""
               UPDATE sensor.battery 
               SET battery_level = {data.battery_level} WHERE id = {sensor_id};""")

    # After that we will update the data on Redis, we will call an internal redis client method that allows us to store data under a key
    return redis.add_sensor(sensor_id, data)

def get_data(redis: Session, ts: Session, sensor_id: int, from_data: str, to_data: str, bucket: str):
    # If no time specifications, we will call an internal redis client method that allows us to get data under a key
    if not from_data and not to_data:
        redis_data =  redis.get_sensor(sensor_id)
        redis_data['id'] = sensor_id
        return redis_data

    # Else, we will search the data on Timescale
    else:
        valid_buckets = ['hour', 'day', 'week', 'month', 'year']
        # If no bucket is specified we will rise and error
        if not bucket or bucket not in valid_buckets:
            raise HTTPException(status_code=400, detail="Bucket is not valid")
        # Else we will filter data by the provided conditions
        if not from_data:
            query = f'''SELECT id, time_bucket('1 {bucket}', last_seen) AS {bucket}, AVG(velocity), AVG(temperature), AVG(humidity), MIN(battery_level)
                                FROM sensor_data WHERE id = {sensor_id} AND last_seen <= '{to_data}';'''
        elif not to_data:
            query = f'''SELECT id, time_bucket('1 {bucket}', last_seen) AS {bucket}, AVG(velocity), AVG(temperature), AVG(humidity), MIN(battery_level)
                                FROM sensor_data WHERE id = {sensor_id} AND  last_seen >= '{from_data}';'''
        else:
            query = f'''SELECT id, time_bucket('1 {bucket}', last_seen) AS {bucket}, AVG(velocity), AVG(temperature), AVG(humidity), MIN(battery_level)
                    FROM sensor_data WHERE id = {sensor_id} AND  last_seen >= '{from_data}' AND last_seen <= '{to_data}'
                    GROUP BY id,{bucket};'''
        ts.getCursor().execute(query)
        return ts.getCursor().fetchall()

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(mongodb: Session, redisdb: Session, latitude, longitude, radius):
    # First get nearest sensor
    sensors = mongodb.get_near_sensors(latitude, longitude, radius)
    # If there is any sensor, add its variable data stored in Redis
    if sensors is not None:
        for i in range(len(sensors)):
            sensor_redis = get_data(redisdb,sensors[i]['id'])
            sensors[i]["temperature"] = sensor_redis["temperature"]
            sensors[i]["humidity"] = sensor_redis["humidity"]
            sensors[i]["battery_level"] = sensor_redis["battery_level"]
            sensors[i]["velocity"] = sensor_redis["velocity"]
            sensors[i]["last_seen"] = sensor_redis["last_seen"]
    return sensors

def search_sensors(db: Session, mongodb: Session, elastic: Session, query: str, size: int, search_type: str):
    # First, in case search type isn't recognized by the database, change it for the equivalent for ElasticSearch
    if search_type == "similar":
        search_type = "fuzzy"
    # Perform search on ElasticSearch
    s_query = {
        "query": {
            str(search_type) : json.loads(query)
        }
    }
    sensors_searched = elastic.search(index_name='sensors', query=s_query)["hits"]["hits"][:size]
    sensors = []
    # Once we have the results, we iterate them and get their corresponding data in MongoDB
    for s in sensors_searched:
        sensor_db = get_sensor_by_name(db, str(s["_source"]["name"]))
        sensors.append(get_sensor(mongodb,sensor_db.id))
    return sensors

def get_temperature_values(mongodb: Session, cassandra : Session):
    # Get temperature stadistic values with Cassandra
    temp_sensors = cassandra.execute("""
        SELECT id,
        MAX(temperature) as max_temperature,
        MIN(temperature) as min_temperature, 
        AVG(temperature) as average_temperature
        FROM sensor.temperature GROUP BY id;""")

    response = []
    # Iterate through sensors and add additional data stored on MongoDB
    for sensor in temp_sensors:
        # First, we will get mongo's data about the sensor
        sensor_data = json.loads(get_sensor(mongodb,sensor.id))
        # Then we will add Cassandra's values
        sensor_data["values"] = [{"max_temperature": sensor.max_temperature,"min_temperature": sensor.min_temperature,"average_temperature": sensor.average_temperature}]
        response.append(sensor_data)

    return {'sensors': response}

def get_sensors_quantity(cassandra: Session):
    # Get quantities of each type with Cassandra
    quantity_sensors = cassandra.execute("""
            SELECT type_sensor, quantity
            FROM sensor.quantity GROUP BY type_sensor;""")

    response = []
    # Reformat returned data
    for sensor in quantity_sensors:
        response.append({"type" : sensor.type_sensor, "quantity" : sensor.quantity})

    return {'sensors': response}

def get_low_battery_sensors(mongodb:Session, cassandra : Session):
    # Get latest battery_level of each sensor with Cassandra db, filter those under 20%
    battery_sensors = cassandra.execute("""
               SELECT id, battery_level
               FROM sensor.battery 
               WHERE battery_level < 0.2 ALLOW FILTERING;""")

    response = []
    # Iterate through sensors and add additional data stored on MongoDB
    for sensor in battery_sensors:
        # First, we will get mongo's data about the sensor
        sensor_data = json.loads(get_sensor(mongodb,sensor.id))
        # Then we will update the latest batter_level
        sensor_data['battery_level'] = round(sensor.battery_level, 2)
        response.append(sensor_data)

    return {'sensors': response}
