#include <iostream>
#include <string>
#include <memory>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <vector>
#include <algorithm>
#include <iomanip>

using json = nlohmann::json;

class BatteryHealthMonitor {
private:
    std::unique_ptr<RdKafka::KafkaConsumer> consumer;
    std::string errstr;
    bool running = true;
    
    // Health monitoring thresholds
    const double LOW_HEALTH_THRESHOLD = 80.0;
    const double CRITICAL_HEALTH_THRESHOLD = 70.0;
    const double HIGH_TEMP_THRESHOLD = 35.0;
    const double CRITICAL_TEMP_THRESHOLD = 40.0;
    const double LOW_VOLTAGE_THRESHOLD = 3.0;
    const double CRITICAL_VOLTAGE_THRESHOLD = 2.8;
    
public:
    auto setup() -> bool {
        auto conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        
        // Your Confluent Cloud configuration
        conf->set("bootstrap.servers", "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092", errstr);
        conf->set("security.protocol", "SASL_SSL", errstr);
        conf->set("sasl.mechanisms", "PLAIN", errstr);
        conf->set("sasl.username", "7HUF4DDIF5DSMWX7", errstr);
        conf->set("sasl.password", "cfltSAbzqfH4158WxkV5FR7EpKW0zJeQdOABf2yt5HAKoO8mwBCbm85is8Nsw4tA", errstr);
        conf->set("session.timeout.ms", "45000", errstr);
        conf->set("group.id", "battery-health-monitors", errstr);
        conf->set("auto.offset.reset", "earliest", errstr);
        conf->set("client.id", "battery-health-consumer", errstr);
        
        consumer.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
        if (!consumer) {
            std::cerr << "Failed to create consumer: " << errstr << std::endl;
            return false;
        }
        
        std::vector<std::string> topics = {"battery-health-data"};
        RdKafka::ErrorCode err = consumer->subscribe(topics);
        if (err) {
            std::cerr << "Failed to subscribe to topics: " << RdKafka::err2str(err) << std::endl;
            return false;
        }
        
        std::cout << "Consumer setup successful!" << std::endl;
        return true;
    }
    
    auto analyzeHealthData(const json& data) -> void {
        try {
            std::string vehicleId = data["vehicle_id"];
            double healthScore = data["health_score"];
            double temperature = data["temperature"];
            double voltage = data["voltage"];
            double current = data["current"];
            std::time_t timestamp = data["timestamp"];
            
            // Convert timestamp to readable format
            std::tm* timeinfo = std::localtime(&timestamp);
            char timeStr[100];
            std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S", timeinfo);
            
            std::cout << "\n=== BATTERY ANALYSIS ===" << std::endl;
            std::cout << "Vehicle: " << vehicleId << std::endl;
            std::cout << "Time: " << timeStr << std::endl;
            std::cout << std::fixed << std::setprecision(2);
            std::cout << "Health Score: " << healthScore << "%" << std::endl;
            std::cout << "Voltage: " << voltage << "V" << std::endl;
            std::cout << "Current: " << current << "A" << std::endl;
            std::cout << "Temperature: " << temperature << "°C" << std::endl;
            
            // Generate alerts based on thresholds
            generateHealthAlerts(vehicleId, healthScore, voltage, temperature, current);
            
        } catch (const json::exception& e) {
            std::cerr << "JSON parsing error: " << e.what() << std::endl;
        }
    }
    
    auto generateHealthAlerts(const std::string& vehicleId, double healthScore, 
                             double voltage, double temperature, double current) -> void {
        std::vector<std::string> alerts;
        std::string priority = "INFO";
        
        // Health Score Analysis
        if (healthScore < CRITICAL_HEALTH_THRESHOLD) {
            alerts.push_back("CRITICAL: Battery health critically low (" + 
                           std::to_string((int)healthScore) + "%)");
            priority = "CRITICAL";
        } else if (healthScore < LOW_HEALTH_THRESHOLD) {
            alerts.push_back("WARNING: Battery health degrading (" + 
                           std::to_string((int)healthScore) + "%)");
            if (priority == "INFO") priority = "WARNING";
        }
        
        // Temperature Analysis
        if (temperature > CRITICAL_TEMP_THRESHOLD) {
            alerts.push_back("CRITICAL: Battery overheating (" + 
                           std::to_string((int)temperature) + "°C)");
            priority = "CRITICAL";
        } else if (temperature > HIGH_TEMP_THRESHOLD) {
            alerts.push_back("WARNING: High battery temperature (" + 
                           std::to_string((int)temperature) + "°C)");
            if (priority == "INFO") priority = "WARNING";
        }
        
        // Voltage Analysis
        if (voltage < CRITICAL_VOLTAGE_THRESHOLD) {
            alerts.push_back("CRITICAL: Battery voltage critically low (" + 
                           std::to_string(voltage) + "V)");
            priority = "CRITICAL";
        } else if (voltage < LOW_VOLTAGE_THRESHOLD) {
            alerts.push_back("WARNING: Low battery voltage (" + 
                           std::to_string(voltage) + "V)");
            if (priority == "INFO") priority = "WARNING";
        }
        
        // Current Analysis
        if (current < -3.0) {
            alerts.push_back("WARNING: High discharge rate (" + 
                           std::to_string(current) + "A)");
            if (priority == "INFO") priority = "WARNING";
        }
        
        // Display alerts
        if (!alerts.empty()) {
            std::cout << "\n🚨 ALERTS FOR " << vehicleId << " [" << priority << "]:" << std::endl;
            for (const auto& alert : alerts) {
                std::cout << "  • " << alert << std::endl;
            }
            
            // Generate maintenance recommendations
            generateMaintenanceRecommendations(vehicleId, healthScore, voltage, temperature, priority);
        } else {
            std::cout << "✅ Battery status: HEALTHY" << std::endl;
        }
    }
    
    auto generateMaintenanceRecommendations(const std::string& vehicleId, double healthScore, 
                                          double voltage, double temperature, const std::string& priority) -> void {
        std::cout << "\n📋 MAINTENANCE RECOMMENDATIONS:" << std::endl;
        
        if (priority == "CRITICAL") {
            std::cout << "  • IMMEDIATE ACTION REQUIRED" << std::endl;
            std::cout << "  • Contact customer immediately" << std::endl;
            std::cout << "  • Schedule emergency service appointment" << std::endl;
            if (temperature > CRITICAL_TEMP_THRESHOLD) {
                std::cout << "  • Vehicle should not be driven - potential safety risk" << std::endl;
            }
        } else if (priority == "WARNING") {
            std::cout << "  • Schedule maintenance within 2 weeks" << std::endl;
            std::cout << "  • Monitor battery performance closely" << std::endl;
            if (healthScore < LOW_HEALTH_THRESHOLD) {
                std::cout << "  • Prepare for battery replacement in next 3-6 months" << std::endl;
            }
        }
        
        // Cost impact analysis
        if (healthScore < CRITICAL_HEALTH_THRESHOLD) {
            std::cout << "\n💰 COST IMPACT:" << std::endl;
            std::cout << "  • Proactive replacement: $8,000-12,000" << std::endl;
            std::cout << "  • Emergency replacement: $15,000-20,000" << std::endl;
            std::cout << "  • Potential savings: $3,000-8,000" << std::endl;
        }
        
        std::cout << "=========================" << std::endl;
    }
    
    auto startMonitoring() -> void {
        std::cout << "🔋 Battery Health Monitor Started!" << std::endl;
        std::cout << "Monitoring real-time battery health data..." << std::endl;
        std::cout << "Press Ctrl+C to stop monitoring\n" << std::endl;
        
        while (running) {
            auto msg = std::unique_ptr<RdKafka::Message>(consumer->consume(1000));
            
            switch (msg->err()) {
                case RdKafka::ERR__TIMED_OUT:
                    // No message received within timeout, continue
                    break;
                    
                case RdKafka::ERR_NO_ERROR: {
                    // Message received successfully
                    std::string payload(static_cast<const char*>(msg->payload()), msg->len());
                    
                    try {
                        json batteryData = json::parse(payload);
                        analyzeHealthData(batteryData);
                    } catch (const json::exception& e) {
                        std::cerr << "Failed to parse JSON: " << e.what() << std::endl;
                    }
                    break;
                }
                
                case RdKafka::ERR__PARTITION_EOF:
                    std::cout << "Reached end of partition" << std::endl;
                    break;
                    
                default:
                    std::cerr << "Consumer error: " << msg->errstr() << std::endl;
                    running = false;
                    break;
            }
        }
        
        std::cout << "Closing consumer..." << std::endl;
        consumer->close();
    }
    
    auto stop() -> void {
        running = false;
    }
};

int main() {
    BatteryHealthMonitor monitor;
    
    if (!monitor.setup()) {
        std::cerr << "Failed to setup consumer" << std::endl;
        return 1;
    }
    
    // Start monitoring (this will run until stopped)
    monitor.startMonitoring();
    
    return 0;
}