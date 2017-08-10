/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.admin.AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.admin.Kafka10AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;

/**
 * Test support class for {@link KafkaMessageChannelBinder}.
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author David Turanski
 * @author Gary Russell
 * @author Soby Chacko
 */
public class Kafka10TestBinder extends AbstractKafkaTestBinder {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Kafka10TestBinder(KafkaBinderConfigurationProperties binderConfiguration) {
		try {
			AdminUtilsOperation adminUtilsOperation = new Kafka10AdminUtilsOperation();
			KafkaTopicProvisioner provisioningProvider =
					new KafkaTopicProvisioner(binderConfiguration, adminUtilsOperation);
			provisioningProvider.afterPropertiesSet();
			KafkaMessageChannelErrorConfigurer errorConfigurer = new KafkaMessageChannelErrorConfigurer(binderConfiguration);
			KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(binderConfiguration,
					provisioningProvider, errorConfigurer);

			binder.setCodec(AbstractKafkaTestBinder.getCodec());
			ProducerListener producerListener = new LoggingProducerListener();
			binder.setProducerListener(producerListener);
			AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
			errorConfigurer.setApplicationContext(context);
			binder.setApplicationContext(context);
			binder.afterPropertiesSet();
			this.setBinder(binder);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Configuration
	@EnableIntegration
	static class Config {

	}

}
