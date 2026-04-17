package io.mitochondria.dltprocessor.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Base64;

@Service
public class DltProcessorService {
    private static final Logger logger = LoggerFactory.getLogger(DltProcessorService.class);

    @KafkaListener(topicPattern = "^.*-dlt$")
    public void processDeadLetters(String deadLetterBase64) {
        String deadLetter = new String(Base64.getDecoder().decode(deadLetterBase64.substring(1, deadLetterBase64.length() - 1)));

        logger.info("Dead letter {} successfully processed", deadLetter);
    }
}
