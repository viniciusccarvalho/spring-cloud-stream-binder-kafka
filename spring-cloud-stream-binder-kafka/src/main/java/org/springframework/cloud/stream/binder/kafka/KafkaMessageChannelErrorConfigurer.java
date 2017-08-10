package org.springframework.cloud.stream.binder.kafka;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;

import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.MessageProducerBinding;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.error.AbstractMessageChannelErrorConfigurer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Vinicius Carvalho
 */
public class KafkaMessageChannelErrorConfigurer extends AbstractMessageChannelErrorConfigurer<ExtendedConsumerProperties<KafkaConsumerProperties>>{

	private final KafkaBinderConfigurationProperties configurationProperties;

	public KafkaMessageChannelErrorConfigurer(KafkaBinderConfigurationProperties configurationProperties) {
		this.configurationProperties = configurationProperties;
	}

	@Override
	public void configure(Binding<MessageChannel> binding) {
		MessageProducerBinding consumerBinding = (MessageProducerBinding) binding;
		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = (ExtendedConsumerProperties<KafkaConsumerProperties>) consumerBinding.getDestination().getProperties();
		KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter = (KafkaMessageDrivenChannelAdapter) consumerBinding.getMessageProducer();
		ErrorInfrastructure errorInfrastructure = getErrorInfrastructure(consumerBinding.getDestination().getName());

		if (extendedConsumerProperties.getMaxAttempts() > 1) {
			kafkaMessageDrivenChannelAdapter.setRetryTemplate(buildRetryTemplate(extendedConsumerProperties));
			kafkaMessageDrivenChannelAdapter.setRecoveryCallback(errorInfrastructure.getRecoverer());
		}
		else {
			kafkaMessageDrivenChannelAdapter.setErrorChannel(errorInfrastructure.getErrorChannel());
		}
	}

	@Override
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return new RawRecordHeaderErrorMessageStrategy();
	}

	@Override
	protected MessageHandler getErrorMessageHandler(final ConsumerDestination destination, final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
		if (extendedConsumerProperties.getExtension().isEnableDlq()) {
			DefaultKafkaProducerFactory<byte[], byte[]> producerFactory = getProducerFactory(
					new ExtendedProducerProperties<>(new KafkaProducerProperties()));
			final KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFactory);
			return new MessageHandler() {

				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					final ConsumerRecord<?, ?> record = message.getHeaders()
							.get(KafkaMessageDrivenChannelAdapter.KAFKA_RAW_DATA, ConsumerRecord.class);
					final byte[] key = record.key() != null ? Utils.toArray(ByteBuffer.wrap((byte[]) record.key()))
							: null;
					final byte[] payload = record.value() != null
							? Utils.toArray(ByteBuffer.wrap((byte[]) record.value())) : null;
					String dlqName = StringUtils.hasText(extendedConsumerProperties.getExtension().getDlqName())
							? extendedConsumerProperties.getExtension().getDlqName()
							: "error." + destination.getName() + "." + group;
					ListenableFuture<SendResult<byte[], byte[]>> sentDlq = kafkaTemplate.send(dlqName,
							record.partition(), key, payload);
					sentDlq.addCallback(new ListenableFutureCallback<SendResult<byte[], byte[]>>() {
						StringBuilder sb = new StringBuilder().append(" a message with key='")
								.append(toDisplayString(ObjectUtils.nullSafeToString(key), 50)).append("'")
								.append(" and payload='")
								.append(toDisplayString(ObjectUtils.nullSafeToString(payload), 50))
								.append("'").append(" received from ")
								.append(record.partition());

						@Override
						public void onFailure(Throwable ex) {
							KafkaMessageChannelErrorConfigurer.this.logger.error(
									"Error sending to DLQ " + sb.toString(), ex);
						}

						@Override
						public void onSuccess(SendResult<byte[], byte[]> result) {
							if (KafkaMessageChannelErrorConfigurer.this.logger.isDebugEnabled()) {
								KafkaMessageChannelErrorConfigurer.this.logger.debug(
										"Sent to DLQ " + sb.toString());
							}
						}

					});
				}
			};
		}
		return null;
	}

	private String toDisplayString(String original, int maxCharacters) {
		if (original.length() <= maxCharacters) {
			return original;
		}
		return original.substring(0, maxCharacters) + "...";
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
	protected String errorsBaseName(ConsumerDestination destination, String group, ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {
		return destination.getName() + "." + group;
	}
}
