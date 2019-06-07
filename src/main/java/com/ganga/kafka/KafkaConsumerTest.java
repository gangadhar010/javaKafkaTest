package com.ganga.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerTest {

	public static void main(String[] args) {
		String kafkaServer = "localhost:9092";
		String topic = "ganga_topic1";
		String offset = "earliest";
		String groupId = "gangaConsumerApplication";
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord record : consumerRecords ) {
				System.out.println(record.toString());
			}
		}
		

	}

}
