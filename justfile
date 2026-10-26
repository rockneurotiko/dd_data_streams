# The tasks a contributor runs. `mix` still owns the build, the default suite
# and the release check; this file only adds what needs a container around it.
set shell := ["bash", "-euo", "pipefail", "-c"]

# List the recipes.
default:
    @just --list

# Start every broker the tiers use, and wait until each one is healthy.
up:
    docker compose up -d --wait --wait-timeout 180

# Stop every broker and drop their state.
down:
    docker compose down

# Run one broker tier: just broker sqs
broker name:
    docker compose up -d --wait --wait-timeout 180 {{name}}
    mix test --only {{name}}
    docker compose stop {{name}}

# The SQS tier: a real BroadwaySQS pipeline against goaws.
sqs: (broker "sqs")

# The Kafka tier: a real BroadwayKafka pipeline against Apache Kafka.
kafka: (broker "kafka")

# The RabbitMQ tier: a real BroadwayRabbitMQ pipeline against RabbitMQ.
rabbitmq: (broker "rabbitmq")

# Every broker tier, one after the other.
brokers: sqs kafka rabbitmq
