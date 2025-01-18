# Create transactions topic
docker exec -it  local-kafka \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic fraud

# Create merchant_updates topic
docker exec -it  advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic merchant_updates

# Create card_updates topic
docker exec -it  advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic card_updates

# Create user_updates topic
docker exec -it  advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic user_updates

# Create loyalty_program_updates topic
docker exec -it  advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic loyalty_program_updates

# Create exchange_rates_updates topic
docker exec -it  advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic exchange_rates_updates

# Create enriched_transactions topic
docker exec -it  advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic enriched_transactions

# Create incomplete_events topic
docker exec -it  advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic incomplete_events

docker exec -it advanced-data-payment-processing-systems-kafka-1 \
  kafka-topics.sh --list \
  --bootstrap-server localhost:9092

