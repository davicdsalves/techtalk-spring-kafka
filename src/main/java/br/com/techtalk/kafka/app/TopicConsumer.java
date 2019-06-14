package br.com.techtalk.kafka.app;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
public class TopicConsumer {

    private static Logger logger = LoggerFactory.getLogger(TopicConsumer.class);

    @KafkaListener(id = "foo01", topics = "topicFoo", groupId = "GROUP_FOO_01")
    public void listenFoo(ConsumerRecord<?, ?> cr) {
        logger.info("log foo:" + cr.toString());
    }

    @KafkaListener(id = "bar02", topicPattern = "topicBar", groupId = "GROUP_BAR_02")
    public void listen(@Payload String foo,
                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        logger.info(String.format("log BAR02: payload[%s], key[%s], partition[%d], topic[%s], timestamp[%d]", foo, key, partition, topic, ts));
    }

    @KafkaListener(id = "bar03ToDLT", topics = "topicBar")
    public void listenBarToDLT(ConsumerRecord<String, String> cr) {
        logger.info("log bar03ToDLT: " + cr.toString());
        if (cr.value().equals("bar01Async")) {
            throw new RuntimeException("falha: " + cr.value());
        }
    }

    @KafkaListener(id = "bar04ConsumerPause", topics = "topicBar")
    public void listenBarPauseConsumer(ConsumerRecord<String, String> cr, Consumer consumer) {
        logger.info("log bar01ToDLT: " + cr.toString());
        var topicPartition = new TopicPartition(cr.topic(), cr.partition());
        if (cr.value().equals("bar01Async")) {
            consumer.pause(Collections.singletonList(topicPartition));
        }
    }

    @KafkaListener(id = "barDLT", topics = "topicBar.DLT")
    public void listenBarDLT(String input) {
        logger.info("Mensagem DLT recebida: " + input);
    }

    @KafkaListener(id = "listenerUser", topics = "topicUser")
    public void listenUser(User user) {
        logger.info("log user: " + user);
    }
}
