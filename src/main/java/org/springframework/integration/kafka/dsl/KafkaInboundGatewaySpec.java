/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.integration.kafka.dsl;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.integration.dsl.ComponentsRegistration;
import org.springframework.integration.dsl.MessageProducerSpec;
import org.springframework.integration.dsl.MessagingGatewaySpec;
import org.springframework.integration.kafka.inbound.KafkaInboundGateway;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * A {@link MessageProducerSpec} implementation for the {@link KafkaMessageDrivenChannelAdapter}.
 *
 * @param <K> the key type.
 * @param <V> the request value type.
 * @param <R> the reply value type.
 * @param <S> the target {@link KafkaInboundGatewaySpec} implementation type.
 *
 * @author Gary Russell
 *
 * @since 3.0
 */
public class KafkaInboundGatewaySpec<K, V, R, S extends KafkaInboundGatewaySpec<K, V, R, S>>
		extends MessagingGatewaySpec<S, KafkaInboundGateway<K, V, R>> {

	KafkaInboundGatewaySpec(AbstractMessageListenerContainer<K, V> messageListenerContainer,
			KafkaTemplate<K, R> kafkaTemplate) {
		super(new KafkaInboundGateway<>(messageListenerContainer, kafkaTemplate));
	}

	/**
	 * Set the message converter to use with a record-based consumer.
	 * @param messageConverter the converter.
	 * @return the spec
	 */
	public S messageConverter(RecordMessageConverter messageConverter) {
		this.target.setMessageConverter(messageConverter);
		return _this();
	}

	/**
	 * Specify a {@link RetryTemplate} instance to wrap
	 * {@code KafkaInboundGateway.IntegrationRecordMessageListener} into
	 * {@link org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter}.
	 * @param retryTemplate the {@link RetryTemplate} to use.
	 * @return the spec
	 */
	public S retryTemplate(RetryTemplate retryTemplate) {
		this.target.setRetryTemplate(retryTemplate);
		return _this();
	}

	/**
	 * A {@link RecoveryCallback} instance for retry operation;
	 * if null, the exception will be thrown to the container after retries are exhausted.
	 * Does not make sense if {@link #retryTemplate(RetryTemplate)} isn't specified.
	 * @param recoveryCallback the recovery callback.
	 * @return the spec
	 */
	public S recoveryCallback(RecoveryCallback<? extends Object> recoveryCallback) {
		this.target.setRecoveryCallback(recoveryCallback);
		return _this();
	}

	/**
	 * A {@link ConcurrentMessageListenerContainer} configuration {@link KafkaInboundGatewaySpec}
	 * extension.
	 * @param <K> the key type.
	 * @param <V> the request value type.
	 * @param <R> the reply value type.
	 */
	public static class KafkaInboundGatewayListenerContainerSpec<K, V, R> extends
			KafkaInboundGatewaySpec<K, V, R, KafkaInboundGatewayListenerContainerSpec<K, V, R>>
			implements ComponentsRegistration {

		private final KafkaMessageListenerContainerSpec<K, V> containerSpec;

		private final KafkaTemplateSpec<K, R> templateSpec;

		KafkaInboundGatewayListenerContainerSpec(KafkaMessageListenerContainerSpec<K, V> containerSpec,
				KafkaTemplateSpec<K, R> templateSpec) {
			super(containerSpec.getContainer(), templateSpec.getTemplate());
			this.containerSpec = containerSpec;
			this.templateSpec = templateSpec;
		}

		/**
		 * Configure a listener container by invoking the {@link Consumer} callback, with a
		 * {@link KafkaMessageListenerContainerSpec} argument.
		 * @param configurer the configurer Java 8 Lambda.
		 * @return the spec.
		 */
		public KafkaInboundGatewayListenerContainerSpec<K, V, R> configureListenerContainer(
				Consumer<KafkaMessageListenerContainerSpec<K, V>> configurer) {
			Assert.notNull(configurer, "The 'configurer' cannot be null");
			configurer.accept(this.containerSpec);
			return _this();
		}

		/**
		 * Configure a template by invoking the {@link Consumer} callback, with a
		 * {@link KafkaTemplateSpec} argument.
		 * @param configurer the configurer Java 8 Lambda.
		 * @return the spec.
		 */
		public KafkaInboundGatewayListenerContainerSpec<K, V, R> configureTemplate(
				Consumer<KafkaTemplateSpec<K, R>> configurer) {
			Assert.notNull(configurer, "The 'configurer' cannot be null");
			configurer.accept(this.templateSpec);
			return _this();
		}

		@Override
		public Map<Object, String> getComponentsToRegister() {
			return Collections.singletonMap(this.containerSpec.getContainer(), this.containerSpec.getId());
		}

	}

}
