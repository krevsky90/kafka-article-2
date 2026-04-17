package io.mitochondria.inventory.confg;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name("inventory-reserved")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "524288000")
                .build();
    }

    //this topic contains messages from 'inventory-reserved' topic that cannot be consumed by notification-service
    @Bean
    public NewTopic inventoryReservedDLTopic() {
        return TopicBuilder.name("inventory-reserved-dlt")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "524288000")
                .build();
    }

    @Bean
    public NewTopic inventoryRejectedTopic() {
        return TopicBuilder.name("inventory-rejected")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "524288000")
                .build();
    }

    //this topic contains messages from 'inventory-rejected' topic that cannot be consumed by notification-service
    @Bean
    public NewTopic inventoryRejectedDLTopic() {
        return TopicBuilder.name("inventory-rejected-dlt")
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
                .config(TopicConfig.RETENTION_BYTES_CONFIG, "524288000")
                .build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                ((consumerRecord, e) -> new TopicPartition(
                        consumerRecord.topic() + "-dlt",
                        consumerRecord.partition()
                ))
        );

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(3000, 3));

        return handler;
    }
}