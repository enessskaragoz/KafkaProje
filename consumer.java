import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {

        // Kafka topic and bootstrap server
        String topic = "messages";
        String bootstrapServer = "localhost:9092";

        // Create a Kafka consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "consumer-group");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singleton(topic));

        // Create a Gson object
        Gson gson = new Gson();

        // Loop to poll records from Kafka
        while (true) {
            // Poll records for 1 second
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            // Loop through the records
            for (ConsumerRecord<String, String> record : records) {
                // Get the key and value of the record
                String key = record.key();
                String value = record.value();

                // Convert the value from JSON string to message object
                Message msg = gson.fromJson(value, Message.class);

                // Print the record
                System.out.println("Received: " + record);

                // Print the name and message
                System.out.println("Name: " + msg.getName());
                System.out.println("Message: " + msg.getMessage());
            }
        }
    }
}
