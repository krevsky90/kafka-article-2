package io.mitochondria.order.service;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.mitochondria.events.order.OrderPlacedEvent;
import io.mitochondria.order.dto.OrderRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.Counter;

import java.util.UUID;

@Service
public class OrderService {
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    //custom metrics (for grafana)
    // will be shown in prometheus like:
    // # HELP orderService_messages_sent_total Total messages sent to kafka
    // # TYPE orderService_messages_sent_total counter
    // orderService_messages_sent_total{application="order-service"} 1.0
    private final Counter sentMessagesCounter;
    private final Counter failedOrdersCounter;
    // latency publish + ack from Kafka
    private final Timer kafkaPublishTimer;

    public OrderService(KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;

        this.sentMessagesCounter =
                Counter.builder("order.messages.sent")
                        .description("Total successfully published kafka messages")
                        .register(meterRegistry);

        this.failedOrdersCounter =
                Counter.builder("order.create.failures")
                        .description("Total failed order creations")
                        .register(meterRegistry);

        this.kafkaPublishTimer =
                Timer.builder("order.kafka.publish.duration")
                        .description("Kafka publish acknowledgement duration")
                        .publishPercentileHistogram()
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(meterRegistry);

    }

    //NOTE: we can use Spring AOP to add custom metric (but only for the whole method)
    @Timed(
            value = "order.create.duration",
            description = "Order creation duration",
            histogram = true,
            percentiles = {0.5, 0.95, 0.99}
    )
    public String placeOrder(OrderRequest orderRequest) {
        String orderId = UUID.randomUUID().toString();
        OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent(
                orderId,
                orderRequest.email(),
                orderRequest.productName(),
                orderRequest.quantity()
        );

        Timer.Sample sample = Timer.start(meterRegistry);

        //NOTE: use whenComplete to get the result once publishing is done
        kafkaTemplate
                .send("order-placed", orderPlacedEvent.getOrderId().toString(), orderPlacedEvent)
                .whenComplete((result, ex) -> {
                    sample.stop(kafkaPublishTimer);
                    if (ex == null) {
                        sentMessagesCounter.increment();    //increase metric
                    } else {
                        failedOrdersCounter.increment();
                    }
                });


        return orderId;
    }
}