package io.mitochondria.inventory.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mitochondria.inventory.event.InventoryRejectedEvent;
import io.mitochondria.inventory.event.InventoryReservedEvent;
import io.mitochondria.inventory.model.OutboxEvent;
import io.mitochondria.inventory.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InventoryReplayerScheduler {
    private final Logger logger = LoggerFactory.getLogger(InventoryReplayerScheduler.class);
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;


    public InventoryReplayerScheduler(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper, KafkaTemplate<String, Object> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }


    @Scheduled(fixedDelay = 5000)
    public void replayOutboxEvents() {
        List<OutboxEvent> events = outboxEventRepository.findBySentFalseOrderByCreatedAtAsc(PageRequest.of(0, 100));
        for (OutboxEvent event : events) {
            try {
                String topic = event.getTopic();
                String payloadJson = event.getPayload();
                //todo: add flexibility
                Object payload = "inventory-reserved".equals(topic)
                        ? objectMapper.readValue(payloadJson, InventoryReservedEvent.class)
                        : objectMapper.readValue(payloadJson, InventoryRejectedEvent.class);

                kafkaTemplate.send(topic, event.getKey(), payload).get();  //NOTE! sync mode to be 100% ensure that message is acked by kafka

                //mark event as sent
                event.setSent(true);
                outboxEventRepository.save(event);
            } catch (Exception ex) {
                logger.error("Failed to replay outbox event: {}", event.getKey(), ex);
            }
        }
    }

}


//CompletableFuture<SendResult<String, Object>> future =
//        kafkaTemplate.send("inventory-rejected", inventoryRejectedEvent.orderID(), inventoryRejectedEvent);
//
//        future.whenComplete((result, throwable) -> {
//                if (throwable != null) {
//                logger.info("Error sending inventory rejected event in thread {}", Thread.currentThread().getName());
//                } else {
//                logger.info("Success sending inventory rejected event in thread {}", Thread.currentThread().getName());
//                }
//                });
//                }
