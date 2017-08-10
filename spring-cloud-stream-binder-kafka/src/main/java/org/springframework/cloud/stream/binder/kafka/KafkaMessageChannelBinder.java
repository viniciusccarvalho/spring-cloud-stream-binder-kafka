/*
 * Copyright 2014-2017 the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.error.BinderErrorConfigurer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * A {@link Binder} that uses Kafka as the underlying middleware.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 * @author Henryk Konsek
 * @author Doug Saus
 * @author Vinicius Carvalho
 */
public class KafkaMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<KafkaConsumerProperties>,
			ExtendedProducerProperties<KafkaProducerProperties>, KafkaTopicProvisioner>
		implements ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties> {

	private final KafkaBinderConfigurationProperties configurationProperties;

	private final Map<String, TopicInformation> topicsInUse = new HashMap<>();

	private ProducerListener<byte[], byte[]> producerListener;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	public KafkaMessageChannelBinder(KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider, BinderErrorConfigurer errorConfigurer) {
		super(false, headersToMap(configurationProperties), provisioningProvider, errorConfigurer);
		this.configurationProperties = configurationProperties;
	}

	private static String[] headersToMap(KafkaBinderConfigurationProperties configurationProperties) {
		String[] headersToMap;
		if (ObjectUtils.isEmpty(configurationProperties.getHeaders())) {
			headersToMap = BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length + configurationProperties.getHeaders().length);
			System.arraycopy(configurationProperties.getHeaders(), 0, combinedHeadersToMap,
					BinderHeaders.STANDARD_HEADERS.length,
					configurationProperties.getHeaders().length);
			headersToMap = combinedHeadersToMap;
		}
		return headersToMap;
	}

	public void setExtendedBindingProperties(KafkaExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	public void setProducerListener(ProducerListener<byte[], byte[]> producerListener) {
		this.producerListener = producerListener;
	}

	Map<String, TopicInformation> getTopicsInUse() {
		return this.topicsInUse;
	}

	@Override
	public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	protected MessageHandler createProducerMessageHandler(final ProducerDestination destination,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) throws Exception {
		final DefaultKafkaProducerFactory<byte[], byte[]> producerFB = getProducerFactory(producerProperties);
		Collection<PartitionInfo> partitions = provisioningProvider.getPartitionsForTopic(
				producerProperties.getPartitionCount(),
				false,
				new Callable<Collection<PartitionInfo>>() {
					@Override
					public Collection<PartitionInfo> call() throws Exception {
						return producerFB.createProducer().partitionsFor(destination.getName());
					}
				});
		this.topicsInUse.put(destination.getName(), new TopicInformation(null, partitions));
		if (producerProperties.getPartitionCount() < partitions.size()) {
			if (this.logger.isInfoEnabled()) {
				this.logger.info("The `partitionCount` of the producer for topic " + destination.getName() + " is "
						+ producerProperties.getPartitionCount() + ", smaller than the actual partition count of "
						+ partitions.size() + " of the topic. The larger number will be used instead.");
			}
		}

		KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFB);
		if (this.producerListener != null) {
			kafkaTemplate.setProducerListener(this.producerListener);
		}
		return new ProducerConfigurationMessageHandler(kafkaTemplate, destination.getName(), producerProperties,
				producerFB);
	}

	private DefaultKafkaProducerFactory<byte[], byte[]> getProducerFactory(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(this.configurationProperties.getRequiredAcks()));
		if (!ObjectUtils.isEmpty(configurationProperties.getProducerConfiguration())) {
			props.putAll(configurationProperties.getProducerConfiguration());
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BATCH_SIZE_CONFIG))) {
			props.put(ProducerConfig.BATCH_SIZE_CONFIG,
					String.valueOf(producerProperties.getExtension().getBufferSize()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.LINGER_MS_CONFIG))) {
			props.put(ProducerConfig.LINGER_MS_CONFIG,
					String.valueOf(producerProperties.getExtension().getBatchTimeout()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG))) {
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
					producerProperties.getExtension().getCompressionType().toString());
		}
		if (!ObjectUtils.isEmpty(producerProperties.getExtension().getConfiguration())) {
			props.putAll(producerProperties.getExtension().getConfiguration());
		}
		return new DefaultKafkaProducerFactory<>(props);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected MessageProducer createConsumerEndpoint(final ConsumerDestination destination, final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {

		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(!anonymous || !extendedConsumerProperties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;
		final ConsumerFactory<?, ?> consumerFactory = createKafkaConsumerFactory(anonymous, consumerGroup,
				extendedConsumerProperties);
		int partitionCount = extendedConsumerProperties.getInstanceCount()
				* extendedConsumerProperties.getConcurrency();

		Collection<PartitionInfo> allPartitions = provisioningProvider.getPartitionsForTopic(partitionCount,
				extendedConsumerProperties.getExtension().isAutoRebalanceEnabled(),
				new Callable<Collection<PartitionInfo>>() {
					@Override
					public Collection<PartitionInfo> call() throws Exception {
						return consumerFactory.createConsumer().partitionsFor(destination.getName());
					}
				});

		Collection<PartitionInfo> listenedPartitions;

		if (extendedConsumerProperties.getExtension().isAutoRebalanceEnabled() ||
				extendedConsumerProperties.getInstanceCount() == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<>();
			for (PartitionInfo partition : allPartitions) {
				// divide partitions across modules
				if ((partition.partition()
						% extendedConsumerProperties.getInstanceCount()) == extendedConsumerProperties
								.getInstanceIndex()) {
					listenedPartitions.add(partition);
				}
			}
		}
		this.topicsInUse.put(destination.getName(), new TopicInformation(group, listenedPartitions));

		Assert.isTrue(!CollectionUtils.isEmpty(listenedPartitions), "A list of partitions must be provided");
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = getTopicPartitionInitialOffsets(
				listenedPartitions);
		final ContainerProperties containerProperties = anonymous
				|| extendedConsumerProperties.getExtension().isAutoRebalanceEnabled()
						? new ContainerProperties(destination.getName())
						: new ContainerProperties(topicPartitionInitialOffsets);
		int concurrency = Math.min(extendedConsumerProperties.getConcurrency(), listenedPartitions.size());
		@SuppressWarnings("rawtypes")
		final ConcurrentMessageListenerContainer<?, ?> messageListenerContainer =
				new ConcurrentMessageListenerContainer(
						consumerFactory, containerProperties) {

			@Override
			public void stop(Runnable callback) {
				super.stop(callback);
			}

		};
		messageListenerContainer.setConcurrency(concurrency);
		if (!extendedConsumerProperties.getExtension().isAutoCommitOffset()) {
			messageListenerContainer.getContainerProperties()
					.setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
			messageListenerContainer.getContainerProperties().setAckOnError(false);
		}
		else {
			messageListenerContainer.getContainerProperties()
					.setAckOnError(isAutoCommitOnError(extendedConsumerProperties));
		}
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"Listened partitions: " + StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}
		final KafkaMessageDrivenChannelAdapter<?, ?> kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter<>(
				messageListenerContainer);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());

		return kafkaMessageDrivenChannelAdapter;
	}



	private ConsumerFactory<?, ?> createKafkaConsumerFactory(boolean anonymous, String consumerGroup,
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, anonymous ? "latest" : "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

		if (!ObjectUtils.isEmpty(configurationProperties.getConsumerConfiguration())) {
			props.putAll(configurationProperties.getConsumerConfiguration());
		}
		if (ObjectUtils.isEmpty(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
		}
		if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getConfiguration())) {
			props.putAll(consumerProperties.getExtension().getConfiguration());
		}
		if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getStartOffset())) {
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					consumerProperties.getExtension().getStartOffset().name());
		}

		return new DefaultKafkaConsumerFactory<>(props);
	}

	private boolean isAutoCommitOnError(ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		return properties.getExtension().getAutoCommitOnError() != null
				? properties.getExtension().getAutoCommitOnError()
				: properties.getExtension().isAutoCommitOffset() && properties.getExtension().isEnableDlq();
	}

	private TopicPartitionInitialOffset[] getTopicPartitionInitialOffsets(
			Collection<PartitionInfo> listenedPartitions) {
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = new TopicPartitionInitialOffset[listenedPartitions
				.size()];
		int i = 0;
		for (PartitionInfo partition : listenedPartitions) {

			topicPartitionInitialOffsets[i++] = new TopicPartitionInitialOffset(partition.topic(),
					partition.partition());
		}
		return topicPartitionInitialOffsets;
	}



	private final class ProducerConfigurationMessageHandler extends KafkaProducerMessageHandler<byte[], byte[]>
			implements Lifecycle {

		private boolean running = true;

		private final DefaultKafkaProducerFactory<byte[], byte[]> producerFactory;

		private ProducerConfigurationMessageHandler(KafkaTemplate<byte[], byte[]> kafkaTemplate, String topic,
				ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
				DefaultKafkaProducerFactory<byte[], byte[]> producerFactory) {
			super(kafkaTemplate);
			setTopicExpression(new LiteralExpression(topic));
			setMessageKeyExpression(producerProperties.getExtension().getMessageKeyExpression());
			setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
			if (producerProperties.isPartitioned()) {
				SpelExpressionParser parser = new SpelExpressionParser();
				setPartitionIdExpression(parser.parseExpression("headers." + BinderHeaders.PARTITION_HEADER));
			}
			if (producerProperties.getExtension().isSync()) {
				setSync(true);
			}
			this.producerFactory = producerFactory;
		}

		@Override
		public void start() {
			try {
				super.onInit();
			}
			catch (Exception e) {
				this.logger.error("Initialization errors: ", e);
				throw new RuntimeException(e);
			}
		}

		@Override
		public void stop() {
			producerFactory.stop();
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}
	}

	public static class TopicInformation {

		private final String consumerGroup;

		private final Collection<PartitionInfo> partitionInfos;

		public TopicInformation(String consumerGroup, Collection<PartitionInfo> partitionInfos) {
			this.consumerGroup = consumerGroup;
			this.partitionInfos = partitionInfos;
		}

		public String getConsumerGroup() {
			return consumerGroup;
		}

		public boolean isConsumerTopic() {
			return consumerGroup != null;
		}

		public Collection<PartitionInfo> getPartitionInfos() {
			return partitionInfos;
		}

	}

}
