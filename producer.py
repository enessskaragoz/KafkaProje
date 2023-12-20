import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class Producer {

    public static void main(String[] args) {

        // Kafka topic and bootstrap server
        String topic = "messages";
        String bootstrapServer = "localhost:9092";

        // Create a Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Create a Gson object
        Gson gson = new Gson();

        // Create a scanner to read user input
        Scanner scanner = new Scanner(System.in);

        // Loop to get user input and send messages to Kafka
        while (true) {
            // Prompt the user to enter a name and a message
            System.out.print("Enter your name: ");
            String name = scanner.nextLine();
            System.out.print("Enter your message: ");
            String message = scanner.nextLine();

            // Create a message object with name and message
            Message msg = new Message(name, message);

            // Convert the message object to JSON string
            String json = gson.toJson(msg);

            // Create a producer record with topic, key and value
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, name, json);

            // Send the record to Kafka
            producer.send(record);

            // Print the record
            System.out.println("Sent: " + record);
        }
    }
}

// A class to represent a message
class Message {
    private String name;
    private String message;

    public Message(String name, String message) {
        this.name = name;
        this.message = message;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
