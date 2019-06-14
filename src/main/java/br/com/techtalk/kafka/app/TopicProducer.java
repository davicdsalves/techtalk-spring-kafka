package br.com.techtalk.kafka.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;

@Component
public class TopicProducer implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(TopicProducer.class);

    private final KafkaTemplate<String, Object> template;
    private KafkaListenerEndpointRegistry registry;

    public TopicProducer(KafkaTemplate<String, Object> template, KafkaListenerEndpointRegistry registry) {
        this.template = template;
        this.registry = registry;
    }

    @Override
    public void run(String... args) throws InterruptedException {
        this.template.send("topicFoo", "keyFoo01", "foo1");
        this.template.send("topicFoo", "keyFoo02", "foo2");
        this.template.send("topicFoo", "keyFoo03", "foo3");
        logger.info("ENVIADAS MENSAGENS PARA FOO");
        Thread.sleep(10_000);

        try {
            this.template.send("topicBar", "keyBar02", "bar2Sync").get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        this.template.send("topicBar", "keyBar01", "bar01Async").addCallback(new BarCallback());
        registry.getListenerContainer("bar03ToDLT").pause();
        logger.info("pausando bar03ToDLT");

        Thread.sleep(5_000);
        this.template.send("topicBar", "keyBar02", "bar02Async").addCallback(new BarCallback());
        Thread.sleep(5_000);

        registry.getListenerContainer("bar03ToDLT").resume();
        logger.info("resumindo bar03ToDLT");

        this.template.send("topicUser", "user01", new User("user01", 1));
    }

    public static class BarCallback implements ListenableFutureCallback<SendResult<String, Object>> {
        private static Logger logger = LoggerFactory.getLogger(BarCallback.class);

        @Override
        public void onFailure(Throwable ex) {
            logger.error("Falha ao enviar mensagem.", ex);
        }

        @Override
        public void onSuccess(SendResult<String, Object> result) {
            logger.info("Sucesso ao enviar mensagem." + result.getProducerRecord().value());
        }
    }
}
