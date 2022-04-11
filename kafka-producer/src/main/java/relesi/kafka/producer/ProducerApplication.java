package relesi.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import relesi.kafka.events.EventsProducer;

@Slf4j
public class ProducerApplication {
    public static void main(String[] args) {
        ProducerApplication producerApplication = new ProducerApplication();
        producerApplication.init();
    }
    private void init(){
        log.info("Starting Application");
        EventsProducer eventsProducer = new EventsProducer();
        eventsProducer.execute();
    }
}
