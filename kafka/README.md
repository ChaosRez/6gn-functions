# Notes

## kafka
## create the topic
```bash
docker exec -it <kafka-container-id> kafka-topics --create --topic releases --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```