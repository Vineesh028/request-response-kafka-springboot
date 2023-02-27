package com.kafka.requestresponse.controller;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.requestresponse.model.Message;

@RestController
@RequestMapping("/message")
public class KafkaController {

	@Autowired
	ReplyingKafkaTemplate<String, Message, Message> replyKafkaTemplate;

	@Value("${kafka.topic.request-topic}")
	String requestTopic;

	@Value("${kafka.topic.requestreply-topic}")
	String requestReplyTopic;

	@PostMapping("/")
	public Message sum(@RequestBody Message request) throws InterruptedException, ExecutionException {

		ProducerRecord<String, Message> record = new ProducerRecord<String, Message>(requestTopic, request);
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
		RequestReplyFuture<String, Message, Message> sendAndReceive = replyKafkaTemplate.sendAndReceive(record);

		SendResult<String, Message> sendResult = sendAndReceive.getSendFuture().get();

		sendResult.getProducerRecord().headers()
				.forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));

		ConsumerRecord<String, Message> consumerRecord = sendAndReceive.get();

		return consumerRecord.value();
	}

}
