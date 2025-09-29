#include <iostream>
#include <string>
#include <memory>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>
#include <libpq-fe.h>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

class DatabaseBatteryProducer {
private:
    std::unique_ptr<RdKafka::Producer> producer;
    std::unique_ptr<RdKafka::Topic> topic;
    std::string errstr;
    PGconn* db_conn;
    
public:
    auto setupKafka() -> bool {
        auto conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        
        // Your Confluent Cloud configuration
        conf->set("bootstrap.servers", "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092", errstr);
        conf->set("security.protocol", "SASL_SSL", errstr);
        conf->set("sasl.mechanisms", "PLAIN", errstr);
        conf->set("sasl.username", "7HUF4DDIF5DSMWX7", errstr);
        conf->set("sasl.password", "cfltSAbzqfH4158WxkV5FR7EpKW0zJeQdOABf2yt5HAKoO8mwBCbm85is8Nsw4tA", errstr);
        conf->set("session.timeout.ms", "45000", errstr);
        conf->set("client.id", "automotive-telematics-gateway", errstr);
        
        producer.reset(RdKafka::Producer::create(conf.get(), errstr));
        if (!producer) {
            std::cerr << "Failed to create producer: " << errstr << std::endl;
            return false;
        }
        
        auto tconf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
        topic.reset(RdKafka::Topic::create(producer.get(), "battery-health-data", tconf.get(), errstr));
        
        if (!topic) {
            std::cerr << "Failed to create topic: " << errstr << std::endl;
            return false;
        }
        
        std::cout << "Automotive Telematics Gateway - Kafka Producer Ready" << std::endl;
        return true;
    }
    
    auto setupDatabase() -> bool {
        const char* conninfo = "host=localhost port=5432 dbname=battery_monitoring user=battery_user password=battery_pass";
        
        db_conn = PQconnectdb(conninfo);
        
        if (PQstatus(db_conn) != CONNECTION_OK) {
            std::cerr << "Database connection failed: " << PQerrorMessage(db_conn) << std::endl;
            PQfinish(db_conn);
            return false;
        }
        
        std::cout << "Connected to Vehicle ECU Database" << std::endl;
        return true;
    }
    
    auto sendBatteryData(const std::string& vehicleId, double voltage, 
                        double current, double temperature, double healthScore,
                        const std::string& alertLevel) -> void {
        json batteryData = {
            {"timestamp", std::time(nullptr)},
            {"vehicle_id", vehicleId},
            {"voltage", voltage},
            {"current", current},
            {"temperature", temperature},
            {"health_score", healthScore},
            {"alert_level", alertLevel},
            {"source", "automotive_telematics_gateway"},
            {"security_validated", true},
            {"encryption_applied", true}
        };
        
        std::string payload = batteryData.dump();
        
        RdKafka::ErrorCode resp = producer->produce(
            topic.get(), 
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(payload.c_str()), 
            payload.size(),
            &vehicleId, 
            nullptr
        );
        
        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to transmit vehicle data: " << RdKafka::err2str(resp) << std::endl;
        } else {
            std::cout << "Transmitted: " << vehicleId << " - Health: " << healthScore 
                     << "%, Alert: " << alertLevel << std::endl;
        }
        
        producer->poll(0);
    }
    
    auto streamVehicleData() -> void {
        std::cout << "=== Automotive Telematics Gateway Started ===" << std::endl;
        std::cout << "Reading from Vehicle ECU Database..." << std::endl;
        std::cout << "Applying ISO 26262 Security Validation..." << std::endl;
        std::cout << "Streaming to Cloud Platform..." << std::endl;
        
        const char* query = R"(
            SELECT vehicle_id, voltage, current, temperature, health_score, alert_level, id
            FROM vehicle_battery_data 
            ORDER BY id 
            LIMIT 200
        )";
        
        PGresult* res = PQexec(db_conn, query);
        
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "Database query failed: " << PQerrorMessage(db_conn) << std::endl;
            PQclear(res);
            return;
        }
        
        int nrows = PQntuples(res);
        std::cout << "Processing " << nrows << " vehicle battery records..." << std::endl;
        
        for (int i = 0; i < nrows; i++) {
            std::string vehicleId = PQgetvalue(res, i, 0);
            double voltage = std::stod(PQgetvalue(res, i, 1));
            double current = std::stod(PQgetvalue(res, i, 2));
            double temperature = std::stod(PQgetvalue(res, i, 3));
            double healthScore = std::stod(PQgetvalue(res, i, 4));
            std::string alertLevel = PQgetvalue(res, i, 5);
            
            // Automotive security validation (simulated)
            if (voltage > 0 && voltage < 5.0 && temperature > -40 && temperature < 100) {
                sendBatteryData(vehicleId, voltage, current, temperature, healthScore, alertLevel);
                
                // Simulate realistic vehicle transmission timing
                std::this_thread::sleep_for(500ms); // 2 transmissions per second
            } else {
                std::cout << "Security validation failed for " << vehicleId 
                         << " - Data rejected" << std::endl;
            }
        }
        
        PQclear(res);
        std::cout << "Vehicle data transmission completed" << std::endl;
    }
    
    auto continuousMonitoring() -> void {
        std::cout << "\n=== Starting Continuous Database Monitoring ===" << std::endl;
        std::cout << "Monitoring for database changes every 30 seconds..." << std::endl;
        
        time_t lastCheck = time(nullptr);
        int updateCounter = 0;
        
        while (updateCounter < 20) { // Run for about 10 minutes (20 * 30 seconds)
            // Simulate real vehicle data updates
            simulateVehicleDataUpdates();
            
            // Check for recent database changes
            streamRecentChanges(lastCheck);
            
            lastCheck = time(nullptr);
            updateCounter++;
            
            std::cout << "Monitoring cycle " << updateCounter << "/20 completed. "
                     << "Next check in 30 seconds..." << std::endl;
            
            std::this_thread::sleep_for(30s); // Check every 30 seconds
        }
        
        std::cout << "Continuous monitoring session completed." << std::endl;
    }
    
    auto simulateVehicleDataUpdates() -> void {
        // Update existing records to simulate vehicle data changes
        std::vector<std::string> updateQueries = {
            "UPDATE vehicle_battery_data SET "
            "voltage = voltage + (random() - 0.5) * 0.1, "
            "temperature = temperature + (random() - 0.5) * 10, "
            "health_score = GREATEST(health_score - random() * 2, 10), "
            "alert_level = CASE "
            "  WHEN temperature + (random() - 0.5) * 10 > 40 THEN 'CRITICAL' "
            "  WHEN temperature + (random() - 0.5) * 10 > 35 OR health_score - random() * 2 < 80 THEN 'WARNING' "
            "  ELSE 'HEALTHY' "
            "END, "
            "updated_at = NOW() "
            "WHERE vehicle_id = 'NASA_BATTERY_1';",
            
            "UPDATE vehicle_battery_data SET "
            "voltage = voltage + (random() - 0.5) * 0.15, "
            "temperature = temperature + (random() - 0.5) * 15, "
            "health_score = GREATEST(health_score - random() * 3, 10), "
            "alert_level = CASE "
            "  WHEN temperature + (random() - 0.5) * 15 > 40 THEN 'CRITICAL' "
            "  WHEN temperature + (random() - 0.5) * 15 > 35 OR health_score - random() * 3 < 80 THEN 'WARNING' "
            "  ELSE 'HEALTHY' "
            "END, "
            "updated_at = NOW() "
            "WHERE vehicle_id = 'NASA_BATTERY_2';",
            
            "UPDATE vehicle_battery_data SET "
            "voltage = voltage + (random() - 0.5) * 0.08, "
            "temperature = temperature + (random() - 0.5) * 12, "
            "health_score = GREATEST(health_score - random() * 1.5, 10), "
            "alert_level = CASE "
            "  WHEN temperature + (random() - 0.5) * 12 > 40 THEN 'CRITICAL' "
            "  WHEN temperature + (random() - 0.5) * 12 > 35 OR health_score - random() * 1.5 < 80 THEN 'WARNING' "
            "  ELSE 'HEALTHY' "
            "END, "
            "updated_at = NOW() "
            "WHERE vehicle_id = 'NASA_BATTERY_3';",
            
            // Add more aggressive degradation for some vehicles to trigger alerts
            "UPDATE vehicle_battery_data SET "
            "temperature = 45 + random() * 5, "
            "health_score = 65 + random() * 10, "
            "alert_level = 'CRITICAL', "
            "updated_at = NOW() "
            "WHERE vehicle_id = 'NASA_BATTERY_4' AND random() < 0.3;",
            
            "UPDATE vehicle_battery_data SET "
            "temperature = 38 + random() * 3, "
            "health_score = 75 + random() * 8, "
            "alert_level = 'WARNING', "
            "updated_at = NOW() "
            "WHERE vehicle_id = 'NASA_BATTERY_5' AND random() < 0.4;"
        };
        
        for (const auto& query : updateQueries) {
            PGresult* res = PQexec(db_conn, query.c_str());
            
            if (PQresultStatus(res) == PGRES_COMMAND_OK) {
                char* rowsAffected = PQcmdTuples(res);
                if (std::string(rowsAffected) != "0") {
                    std::cout << "Updated vehicle data - " << rowsAffected << " records affected" << std::endl;
                }
            } else {
                std::cerr << "Update failed: " << PQerrorMessage(db_conn) << std::endl;
            }
            PQclear(res);
        }
        
        // Also insert some new records with varied alert levels
        if (rand() % 3 == 0) { // 33% chance
            double temp = 20 + (rand() % 30);
            double health = 60 + (rand() % 40);
            std::string alertLevel = "HEALTHY";
            
            if (temp > 40) alertLevel = "CRITICAL";
            else if (temp > 35 || health < 80) alertLevel = "WARNING";
            
            std::string insertQuery = 
                "INSERT INTO vehicle_battery_data (vehicle_id, voltage, current, temperature, health_score, alert_level) "
                "VALUES ('LIVE_VEHICLE_" + std::to_string(rand() % 5 + 1) + "', " +
                std::to_string(3.6 + (rand() % 100) / 1000.0) + ", " +
                std::to_string(-2.0 + (rand() % 100) / 100.0) + ", " +
                std::to_string(temp) + ", " +
                std::to_string(health) + ", " +
                "'" + alertLevel + "');";
            
            PGresult* res = PQexec(db_conn, insertQuery.c_str());
            if (PQresultStatus(res) == PGRES_COMMAND_OK) {
                std::cout << "New vehicle joined fleet - Alert Level: " << alertLevel << std::endl;
            }
            PQclear(res);
        }
    }
    
    auto streamRecentChanges(time_t lastCheck) -> void {
        // Convert time_t to string for SQL query
        char timeStr[100];
        struct tm* timeinfo = localtime(&lastCheck);
        strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S", timeinfo);
        
        std::string query = 
            "SELECT vehicle_id, voltage, current, temperature, health_score, alert_level, updated_at "
            "FROM vehicle_battery_data "
            "WHERE updated_at > '" + std::string(timeStr) + "' "
            "ORDER BY updated_at DESC;";
        
        PGresult* res = PQexec(db_conn, query.c_str());
        
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "Query for recent changes failed: " << PQerrorMessage(db_conn) << std::endl;
            PQclear(res);
            return;
        }
        
        int nrows = PQntuples(res);
        if (nrows > 0) {
            std::cout << "Detected " << nrows << " database changes - streaming updates..." << std::endl;
            
            for (int i = 0; i < nrows; i++) {
                std::string vehicleId = PQgetvalue(res, i, 0);
                double voltage = std::stod(PQgetvalue(res, i, 1));
                double current = std::stod(PQgetvalue(res, i, 2));
                double temperature = std::stod(PQgetvalue(res, i, 3));
                double healthScore = std::stod(PQgetvalue(res, i, 4));
                std::string alertLevel = PQgetvalue(res, i, 5);
                std::string updateTime = PQgetvalue(res, i, 6);
                
                // Update alert level based on current values
                if (temperature > 40) {
                    alertLevel = "CRITICAL";
                } else if (temperature > 35 || healthScore < 80) {
                    alertLevel = "WARNING";
                } else {
                    alertLevel = "HEALTHY";
                }
                
                sendBatteryData(vehicleId, voltage, current, temperature, healthScore, alertLevel);
                std::this_thread::sleep_for(100ms); // Small delay between transmissions
            }
        } else {
            std::cout << "No database changes detected in this monitoring cycle." << std::endl;
        }
        
        PQclear(res);
    }
    
    ~DatabaseBatteryProducer() {
        if (producer) {
            producer->flush(10000);
        }
        if (db_conn) {
            PQfinish(db_conn);
        }
    }
};

int main() {
    DatabaseBatteryProducer gateway;
    
    if (!gateway.setupDatabase()) {
        std::cerr << "Failed to connect to vehicle ECU database" << std::endl;
        return 1;
    }
    
    if (!gateway.setupKafka()) {
        std::cerr << "Failed to initialize telematics gateway" << std::endl;
        return 1;
    }
    
    // Stream historical data first
    gateway.streamVehicleData();
    
    // Start continuous monitoring for changes
    gateway.continuousMonitoring();
    
    std::cout << "\nAutomotive Telematics Gateway - Mission Complete" << std::endl;
    return 0;
}