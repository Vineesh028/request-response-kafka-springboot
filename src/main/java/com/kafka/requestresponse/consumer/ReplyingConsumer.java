package com.kafka.requestresponse.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import com.kafka.requestresponse.model.Message;

@Component
public class ReplyingConsumer {

	@KafkaListener(topics = "${kafka.topic.request-topic}")
	@SendTo
	public Message listen(Message request) throws InterruptedException {

		request.setAdditionalProperty("Greeting : ", "Hello, " + request.getName());
		return request;
	}

}