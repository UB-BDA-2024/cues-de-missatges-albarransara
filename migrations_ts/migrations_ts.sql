-- First we create a standard SQL table
CREATE TABLE IF NOT EXISTS sensor_data (
        id INT NOT NULL,
        last_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        velocity FLOAT,
        temperature FLOAT,
        humidity FLOAT,
        battery_level FLOAT NOT NULL,
        CONSTRAINT "pkey" PRIMARY KEY (id,last_seen)
    );

-- Then we convert it to a hypertable
--SELECT create_hypertable('sensor_data', by_range('last_seen'));
SELECT create_hypertable('sensor_data', 'last_seen',if_not_exists => true);
CREATE UNIQUE INDEX time ON sensor_data(id, last_seen)

