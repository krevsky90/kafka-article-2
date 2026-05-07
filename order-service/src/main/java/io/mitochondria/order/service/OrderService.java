package io.mitochondria.order.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.mitochondria.events.order.OrderPlacedEvent;
import io.mitochondria.order.dto.OrderRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.Counter;

import java.util.UUID;

@Service
public class OrderService {
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;
    //custom metric (for grafana)
    // will be shown in prometheus like:
    // # HELP orderService_messages_sent_total Total messages sent to kafka
    // # TYPE orderService_messages_sent_total counter
    // orderService_messages_sent_total{application="order-service"} 1.0
    private final Counter sentMessagesCounter;

    public OrderService(KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate, MeterRegistry registry) {
        this.kafkaTemplate = kafkaTemplate;
        this.sentMessagesCounter = Counter.builder("orderService.messages.sent")
                .description("Total messages sent to kafka")
                .register(registry);
    }

    public String placeOrder(OrderRequest orderRequest) {
        String orderId = UUID.randomUUID().toString();
        OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent(
            orderId,
            orderRequest.email(),
            orderRequest.productName(),
            orderRequest.quantity()
        );

        kafkaTemplate.send("order-placed", orderPlacedEvent.getOrderId().toString(), orderPlacedEvent);

        sentMessagesCounter.increment();    //increase metric

        return orderId;
    }
}