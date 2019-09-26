/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package eu.solutions.a2.cdc.oracle;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import eu.solutions.a2.cdc.oracle.avro.Envelope;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

public class KafkaSingleton implements SendMethodIntf {

	private static final Logger LOGGER = Logger.getLogger(KafkaSingleton.class);
	private static final String SECURITY_SSL = "SSL";
	private static final String SECURITY_SASL_SSL = "SASL_SSL";

	private static KafkaSingleton instance;

	/** Kafka topic */
	private String kafkaTopic = null;
	/**  Kafka producer */
	private Producer<String, String> kafkaProducer;

	private static final ObjectWriter writer = new ObjectMapper()
//			.enable(SerializationFeature.INDENT_OUTPUT)
			.writer();

	private KafkaSingleton() {}

	public static KafkaSingleton getInstance() {
		if (instance == null) {
			instance = new KafkaSingleton();
		}
		return instance;
	}

	public void sendData(final String messageKey, final Envelope envelope) {
		final long startTime = System.currentTimeMillis();
		Runnable task = () -> {
			envelope.getPayload().setTs_ms(startTime);
			try {
				final String messageData = writer.writeValueAsString(envelope);
				ProducerRecord<String, String> record = 
						new ProducerRecord<>(kafkaTopic, messageKey, messageData);
				kafkaProducer.send(
						record,
						(metadata, exception) -> {
							if (metadata == null) {
								// Error occured
								LOGGER.error("Exception while sending " + messageKey + " to Kafka." );
								LOGGER.error("Message data are:\n\t" + messageData);
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(exception));
							} else {
								CommonJobSingleton.getInstance().addRecordData(
										messageData.getBytes().length,
										System.currentTimeMillis() - startTime);
							}
						});
			} catch (JsonProcessingException jpe) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(jpe));
			}
		};
		Thread thread = new Thread(task);
		thread.setName(messageKey);
		thread.setDaemon(true);
		thread.start();
	}

	public void shutdown() {
		if (kafkaProducer != null) {
			kafkaProducer.flush();
			kafkaProducer.close();
		} else {
			LOGGER.fatal("Attempt to close non-initialized Kafka producer!");
			System.exit(1);
		}
	}

	public void parseSettings(final Properties props, final String configPath, final int exitCode) {
		kafkaTopic = props.getProperty("a2.kafka.topic");
		if (kafkaTopic == null || "".equals(kafkaTopic)) {
			LOGGER.fatal("a2.kafka.topic parameter must set in configuration file " + configPath);
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}

		String kafkaServers = props.getProperty("a2.kafka.servers");
		if (kafkaServers == null || "".equals(kafkaServers)) {
			LOGGER.fatal("a2.kafka.servers parameter must set in configuration file " + configPath);
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}

		String kafkaClientId = props.getProperty("a2.kafka.client.id");
		if (kafkaClientId == null || "".equals(kafkaClientId)) {
			LOGGER.fatal("a2.kafka.client.id parameter must set in configuration file " + configPath);
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}

		/** Proprties for building Kafka producer */
		Properties kafkaProps = new Properties();
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientId);
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		final String useSSL = props.getProperty("a2.kafka.security.protocol", "").trim();
		if (SECURITY_SSL.equalsIgnoreCase(useSSL)) {
			kafkaProps.put("security.protocol", SECURITY_SSL);
			kafkaProps.put("ssl.truststore.location", props.getProperty("a2.kafka.security.truststore.location"));
			kafkaProps.put("ssl.truststore.password", props.getProperty("a2.kafka.security.truststore.password"));
		} else if (SECURITY_SASL_SSL.equalsIgnoreCase(useSSL)) {
			kafkaProps.put("security.protocol", SECURITY_SASL_SSL);
			kafkaProps.put("ssl.truststore.location", props.getProperty("a2.kafka.security.truststore.location"));
			kafkaProps.put("ssl.truststore.password", props.getProperty("a2.kafka.security.truststore.password"));
			kafkaProps.put("sasl.mechanism", "PLAIN");
			kafkaProps.put("sasl.jaas.config", props.getProperty("a2.security.jaas.config"));
		}

		String optParam = null;
		optParam = props.getProperty("a2.kafka.compression.type", "").trim();
		if (!"".equals(optParam)) {
			kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, optParam);
		} else {
			/** Set to gzip by default */
			kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
		}

		optParam = props.getProperty("a2.kafka.batch.size", "").trim();
		if (!"".equals(optParam)) {
			kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, optParam);
		}
		optParam = props.getProperty("a2.kafka.linger.ms", "").trim();
		if (!"".equals(optParam)) {
			kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, optParam);
		}
		optParam = props.getProperty("a2.kafka.acks", "").trim();
		if (!"".equals(optParam)) {
			kafkaProps.put(ProducerConfig.ACKS_CONFIG, optParam);
		}
		optParam = props.getProperty("a2.kafka.max.request.size", "").trim();
		if (!"".equals(optParam)) {
			kafkaProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, optParam);
		}
		optParam = props.getProperty("a2.kafka.buffer.memory", "").trim();
		if (!"".equals(optParam)) {
			kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, optParam);
		}
		optParam = props.getProperty("a2.kafka.retries", "").trim();
		if (!"".equals(optParam)) {
			kafkaProps.put(ProducerConfig.RETRIES_CONFIG, optParam);
		}

		// Initialize connection to Kafka
		kafkaProducer = new KafkaProducer<>(kafkaProps);
	}


}
