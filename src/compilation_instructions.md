# Compilation Instructions
## Requirements
- librdkafka-dev
- libpq-dev
- nlohmann-json3-dev

## Compile Commands
```bash
g++ -std=c++14 -I/usr/include/postgresql battery_producer.cpp -lrdkafka++ -lpq -o battery_producer
g++ -std=c++14 battery_consumer.cpp -lrdkafka++ -o battery_consumer
```
