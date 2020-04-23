
from kafka import KafkaConsumer
import time

# Kafka consumer to test data moved to topic


class ConsumerServer:

    def __init__(self, topic, **kwargs):
        # Configure a Kafka Consumer
        super().__init__(**kwargs)
        self.topic = topic
        self.subscribe(topics=topic)
        self.consumer = KafkaConsumer(
            self.topic,
            group_id=self.group_id,
            bootstrap_servers=[bootstrap_servers],
            request_timeout_ms=1000,
            auto_offset_reset="earliest",
            max_poll_records=10
        )

    def consume(self):
        try:
            while True:
                # Poll data for values
                msg = self.consumer.poll()
                if msg is not None:
                    print(
                    "Message consumed: topic={0}, partition={1}, offset={2}, key={3}, value={4}".format(
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        msg.key().decode("utf-8"),
                        msg.value().decode("utf-8"),
                        )
                    )
                else:
                    print("No message received by consumer")

                    time.sleep(0.5)
        except:
            print("Closing consumer")
            self.close()


if __name__ == "__main__":
    consumer = ConsumerServer(topic="police.received.calls",
                              bootstrap_servers="localhost:9092",
                              request_timeout_ms=1000,
                              auto_offset_reset="earliest",
                              max_poll_records=10)
    consumer.consume()
