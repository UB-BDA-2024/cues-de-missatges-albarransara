from fastapi import APIRouter, Depends, HTTPException,Request
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from app.elasticsearch_client import ElasticsearchClient
from app.timescale import Timescale
from app.cassandra_client import CassandraClient
from . import schemas, repository

# Dependency to get db session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_timescale():
    ts = Timescale()
    try:
        yield ts
    finally:
        ts.close()

# Dependency to get redis client
def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()

# Dependency to get mongodb client
def get_mongodb_client():
    mongodb = MongoDBClient(host="mongodb")
    try:
        yield mongodb
    finally:
        mongodb.close()

# Dependency to get elastic_search client
def get_elastic_search():
    es = ElasticsearchClient(host="elasticsearch")
    try:
        yield es
    finally:
        es.close()

# Dependency to get cassandra client
def get_cassandra_client():
    cassandra = CassandraClient(hosts=["cassandra"])
    try:
        yield cassandra
    finally:
        cassandra.close()


router = APIRouter(
    prefix="/sensors",
    responses={404: {"description": "Not found"}},
    tags=["sensors"],
)


# 🙋🏽‍♀️ Add here the route to get a list of sensors near to a given location
@router.get("/near")
def get_sensors_near(latitude: float, longitude: float, radius:float, redis_client: RedisClient = Depends(get_redis_client),mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    return repository.get_sensors_near(mongodb=mongodb_client, redisdb = redis_client, latitude=latitude, longitude=longitude, radius=radius)

# 🙋🏽‍♀️ Add here the route to search sensors by query to Elasticsearch
# Parameters:
# - query: string to search
# - size (optional): number of results to return
# - search_type (optional): type of search to perform
# - db: database session
# - mongodb_client: mongodb client
@router.get("/search")
def search_sensors(query: str, size: int = 10, search_type: str = "match", db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), es: ElasticsearchClient = Depends(get_elastic_search)):
    return repository.search_sensors(db=db,mongodb=mongodb_client,elastic=es,query=query, size=size, search_type=search_type)

# 🙋🏽‍♀️ Add here the route to get the temperature values of a sensor
@router.get("/temperature/values")
def get_temperature_values(mongo_client: MongoDBClient = Depends(get_mongodb_client), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_temperature_values(mongodb=mongo_client, cassandra=cassandra_client)

@router.get("/quantity_by_type")
def get_sensors_quantity(cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_sensors_quantity(cassandra=cassandra_client)

@router.get("/low_battery")
def get_low_battery_sensors(mongo_client:  MongoDBClient = Depends(get_mongodb_client), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_low_battery_sensors(mongodb=mongo_client, cassandra=cassandra_client)

# 🙋🏽‍♀️ Add here the route to get all sensors
@router.get("")
def get_sensors(db: Session = Depends(get_db)):
    return repository.get_sensors(db)


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), cassandra_client: CassandraClient = Depends(get_cassandra_client), elasticsearch_client : ElasticsearchClient = Depends(get_elastic_search)):
    db_sensor = repository.get_sensor_by_name(db, sensor.name)
    if db_sensor:
        raise HTTPException(status_code=400, detail="Sensor with same name already registered")
    return repository.create_sensor(db=db, mongodb= mongodb_client, elastic=elasticsearch_client, cassandra=cassandra_client, sensor=sensor)

# 🙋🏽‍♀️ Add here the route to get a sensor by id
@router.get("/{sensor_id}")
def get_sensor(sensor_id: int, mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor(mongodb_client, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return db_sensor

# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.delete_sensor(db=db, sensor_id=sensor_id)

# 🙋🏽‍♀️ Add here the route to update a sensor
@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, data: schemas.SensorData, mongo: Session = Depends(get_mongodb_client), cassandra_client: CassandraClient = Depends(get_cassandra_client) ,redis_client: RedisClient = Depends(get_redis_client), timescale: Timescale = Depends(get_timescale)):
    # First, check if sensor is on the database
    db_sensor = repository.get_sensor(mongo, sensor_id)
    # If the sensor is not on the database, we will rise an error
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    # Else, we will post the new data
    else:
        return repository.record_data(redis=redis_client, ts=timescale, cassandra=cassandra_client, sensor_id=sensor_id, data=data)

# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(sensor_id: int, request: Request, mongo: Session = Depends(get_mongodb_client), redis_client: RedisClient = Depends(get_redis_client), timescale: Timescale = Depends(get_timescale)):
    # First, check if sensor is on the database
    db_sensor = repository.get_sensor(mongo, sensor_id)
    # If the sensor is not on the database, we will rise an error
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    # Else we will return the data
    else:
        return repository.get_data(redis=redis_client, ts=timescale, sensor_id=sensor_id, from_data=request.query_params.get('from',None), to_data=request.query_params.get('to',None), bucket=request.query_params.get('bucket',None))

