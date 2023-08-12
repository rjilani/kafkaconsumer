import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Driver {
    public static void main(String[] args) throws InterruptedException {

        String topicName = "quickstart-events";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        if (args.length == 3) {
            topicName = args[0];
            System.out.println("Topic Name: " +  args[0]);
            System.out.println("group.id: " +  args[1]);
            System.out.println("auto.offset.reset: " +  args[2]);
            props.put("group.id", args[1]);
            props.put("auto.offset.reset", args[2]);

        } else {
            props.put("group.id", "group1");

            System.out.println("Topic Name: " +  topicName);
            System.out.println("group.id: " +  "group1");
            System.out.println("auto.offset.reset: " +  "latest");
        }

        Thread.sleep(5000);

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("Topic - %s, Partition - %d, offset = %d, key = %s, value = %s%n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
        }

    }

}
