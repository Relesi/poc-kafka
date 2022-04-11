package relesi.kafka.events;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class EventsProducer {

    private final Producer<String, String> producer;

    public EventsProducer() {
        producer = produceCreate();
    }

    private Producer<String, String> produceCreate() {
        if (producer != null) {
            return null;
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        return new KafkaProducer<String, String>(properties);
    }

    public void execute() {
        String key = UUID.randomUUID().toString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String message = sdf.format(new Date());
        message += "|" + key;
        message += "|NEW_MESSAGE";

        log.info("Starting message sending");
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("EventRecord", key, message);
        producer.send(record);
        producer.flush();
        producer.close();
        log.info("Message sent successfully [{}]", message);
    }
}
