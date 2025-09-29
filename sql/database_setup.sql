-- Database Setup for Automotive Battery Health Monitoring
-- Competition: Confluent Developer Day

-- Create database (run as postgres user)
-- CREATE DATABASE battery_monitoring;

-- Create user with appropriate permissions
-- CREATE USER battery_user WITH PASSWORD 'battery_pass';
-- GRANT ALL PRIVILEGES ON DATABASE battery_monitoring TO battery_user;

-- Connect to battery_monitoring database before running below commands

-- Create main battery data table
CREATE TABLE IF NOT EXISTS vehicle_battery_data (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    voltage DECIMAL(8,3),
    current DECIMAL(8,3),
    temperature DECIMAL(6,2),
    health_score DECIMAL(5,2),
    alert_level VARCHAR(20),
    timestamp BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_vehicle_id ON vehicle_battery_data(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_created_at ON vehicle_battery_data(created_at);
CREATE INDEX IF NOT EXISTS idx_updated_at ON vehicle_battery_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_alert_level ON vehicle_battery_data(alert_level);

-- Sample query to verify setup
-- SELECT COUNT(*) FROM vehicle_battery_data;
