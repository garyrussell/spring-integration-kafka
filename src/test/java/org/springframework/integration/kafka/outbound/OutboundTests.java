/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.integration.kafka.outbound;

import java.util.Collections;

import kafka.serializer.Encoder;

import org.junit.Test;

import org.springframework.integration.kafka.serializer.common.StringEncoder;
import org.springframework.integration.kafka.support.KafkaProducerContext;
import org.springframework.integration.kafka.support.ProducerConfiguration;
import org.springframework.integration.kafka.support.ProducerFactoryBean;
import org.springframework.integration.kafka.support.ProducerMetadata;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Gary Russell
 *
 */
public class OutboundTests {

	@Test
	public void test() throws Exception {
		KafkaProducerContext<String, String> kafkaProducerContext = new KafkaProducerContext<String, String>();
		ProducerMetadata<String, String> producerMetadata = new ProducerMetadata<String, String>("test");
		producerMetadata.setValueClassType(String.class);
		producerMetadata.setKeyClassType(String.class);
		Encoder<String> encoder = new StringEncoder<String>();
		producerMetadata.setValueEncoder(encoder);
		producerMetadata.setKeyEncoder(encoder);
		ProducerFactoryBean<String, String> producer = new ProducerFactoryBean<String, String>(producerMetadata, "localhost:9092");
		ProducerConfiguration<String, String> config = new ProducerConfiguration<String, String>(producerMetadata, producer.getObject());
		kafkaProducerContext.setProducerConfigurations(Collections.singletonMap("test", config));
		KafkaProducerMessageHandler<String, String> handler = new KafkaProducerMessageHandler<String, String>(kafkaProducerContext);
		handler.handleMessage(MessageBuilder.withPayload("foo")
				.setHeader("messagekey", "3")
				.setHeader("topic", "test")
				.build());
	}

}
