package br.com.techtalk.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

 	@Bean
	public NewTopic topicFoo() {
		return new NewTopic("topicFoo", 10, (short) 1);
	}

	@Bean
	public NewTopic topicBar() {
		return new NewTopic("topicBar", 10, (short) 1);
	}

	@Bean
	public NewTopic topicBarDLT() {
		return new NewTopic("topicBar.DLT", 10, (short) 1);
	}

	@Bean
	public NewTopic topicFooDLT() {
		return new NewTopic("topicFoo.DLT", 10, (short) 1);
	}

	@Bean
	public NewTopic topicUser() {
		return new NewTopic("topicUser", 10, (short) 1);
	}

	@Bean
	public RecordMessageConverter converter() {
		return new StringJsonMessageConverter();
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory,
			KafkaTemplate<Object, Object> template) {

		var factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		//SeekToCurrentErrorHandler: descarta proximas mensagems do poll(), e reseta o offset para que as mensagens sejam recebidas no proximo poll().
		factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(template), 3));
		return factory;
	}
}
