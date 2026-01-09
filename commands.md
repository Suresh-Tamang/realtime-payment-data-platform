## 1. creating a (payments.raw) kafka topic
A Topic is a category or feed name to which records are stored and published. If Kafka is a massive library, a Topic is a specific bookshelf dedicated to one subject (e.g., "Payments," "User-Logins," or "Weather-Updates").
```
docker exec -it kafka kafka-topics --create --topic payments.raw --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```
## 2. Creating (payments.deadletter) topic
A "Dead Letter" topic is a safety net. When your Spark or Kafka application tries to process a message from payments.raw and fails (e.g., the data is corrupted, or the "Price" is a string instead of a number), instead of crashing the whole system, you send that "bad" message to the payments.deadletter topic.
```
docker exec -it kafka kafka-topics --create --topic payments.deadletter --bootstrap-server kafka:9092
```
## 3. Checking the kafka topic list
```
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
```
## 4. Get details (Partitions / Replication)
```
docker exec -it kafka kafka-topics --describe --bootstrap-server kafka:9092
```

## 5. Delete kafka topics
```
docker exec -it kafka kafka-topics --delete --topic payments.raw --bootstrap-server localhost:9092
```