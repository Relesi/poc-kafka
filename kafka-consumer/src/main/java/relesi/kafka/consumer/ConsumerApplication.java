package relesi.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import relesi.kafka.events.EventConsumer;

@Slf4j
public class ConsumerApplication {

    public static void main(String[] args) {
        ConsumerApplication consumerApplication = new ConsumerApplication();
        consumerApplication.init();
    }

    private void init() {
        log.info("Starting application");
        EventConsumer eventConsumer = new EventConsumer();
        eventConsumer.execute();
    }
}