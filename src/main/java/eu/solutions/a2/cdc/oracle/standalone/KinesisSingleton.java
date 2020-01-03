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

package eu.solutions.a2.cdc.oracle.standalone;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import eu.solutions.a2.cdc.oracle.standalone.avro.Envelope;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.GzipUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
	
public class KinesisSingleton implements SendMethodIntf {

	private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSingleton.class);

	private static KinesisSingleton instance;

	/** Kinesis stream name */
	private String streamName = null;
	/**  Kinesis Async client */
	private KinesisAsyncClient kinesisClient;
	/** a2.kinesis.file.size.threshold */
	private int fileSizeThreshold = 512;

	private static final int KINESIS_BATCH_SIZE = 500;
	private static final int KINESIS_BATCH_MAX_BYTES = 4_500_000;

	private static final ObjectWriter writer = new ObjectMapper()
//			.enable(SerializationFeature.INDENT_OUTPUT)
			.writer();

	private KinesisSingleton() {}

	public static KinesisSingleton getInstance() {
		if (instance == null) {
			instance = new KinesisSingleton();
		}
		return instance;
	}

	public void sendData(final String messageKey, final Envelope envelope) {
		long startTime = System.currentTimeMillis();
		Runnable task = () -> {
			envelope.getPayload().setTs_ms(startTime);
			try {
				byte[] messageData = writer.writeValueAsBytes(envelope);
				if (messageData.length > fileSizeThreshold) {
					messageData = GzipUtil.compress(writer.writeValueAsString(envelope));
				}
				final int messageLength = messageData.length;
				PutRecordRequest putRecordRequest  = PutRecordRequest.builder()
						.streamName(streamName)
						.partitionKey(messageKey)
						.data(SdkBytes.fromByteArray(messageData))
						.build();
				kinesisClient.putRecord(putRecordRequest)
					.whenComplete((resp, err) -> {
						if (resp != null) {
							CommonJobSingleton.getInstance().addRecordData(
									messageLength,
									System.currentTimeMillis() - startTime);
						} else {
							LOGGER.error("Exception while sending {} to Kinesis.", messageKey);
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(new Exception(err)));
						}
				});
			} catch (IOException ioe) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			}
		};
		Thread thread = new Thread(task);
		thread.setName(messageKey);
		thread.setDaemon(true);
		thread.start();
	}

	public void shutdown() {
		if (kinesisClient != null) {
			kinesisClient.close();
		} else {
			LOGGER.error("Attempt to close non-initialized Kinesis client!");
			System.exit(1);
		}
	}

	public void parseSettings(final Properties props, final String configPath, final int exitCode) {
		streamName = props.getProperty("a2.kinesis.stream", "");
		if (streamName == null || "".equals(streamName)) {
			LOGGER.error("a2.kinesis.stream parameter must set in configuration file {}", configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}

		String region = props.getProperty("a2.kinesis.region", "");
		if (region == null || "".equals(region)) {
			LOGGER.error("a2.kinesis.region parameter must set in configuration file {}", configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}

		String accessKey = props.getProperty("a2.kinesis.access.key", "");
		if (accessKey == null || "".equals(accessKey)) {
			LOGGER.error("a2.kinesis.access.key parameter must set in configuration file {}", configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}

		String accessSecret = props.getProperty("a2.kinesis.access.secret", "");
		if (accessSecret == null || "".equals(accessSecret)) {
			LOGGER.error("a2.kinesis.access.secret parameter must set in configuration file {}", configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}

		// The maxConnections parameter can be used to control the degree of
        // parallelism when making HTTP requests.
		int maxConnections = 50;
		String maxConnectionsString = props.getProperty("a2.kinesis.max.connections", "");
		if (maxConnectionsString != null && !"".equals(maxConnectionsString)) {
			try {
				maxConnections = Integer.parseInt(maxConnectionsString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.kinesis.max.connections -> {}", maxConnectionsString);
				LOGGER.warn("Setting it to 50");
			}
		}

		// Request timeout milliseconds
		long requestTimeout = 30000;
		String requestTimeoutString = props.getProperty("a2.kinesis.request.timeout", "");
		if (requestTimeoutString != null && !"".equals(requestTimeoutString)) {
			try {
				requestTimeout = Integer.parseInt(requestTimeoutString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.kinesis.request.timeout -> {}", requestTimeoutString);
				LOGGER.warn("Setting it to 30000");
			}
		}

		AwsCredentials awsCreds = AwsBasicCredentials.create(accessKey, accessSecret);
		AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(awsCreds);

		kinesisClient = KinesisAsyncClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(Region.of(region))
				.httpClientBuilder(NettyNioAsyncHttpClient.builder()
						.maxConcurrency(maxConnections)
						.maxPendingConnectionAcquires(10_000)
						.writeTimeout(Duration.ofMillis(requestTimeout))
						.readTimeout(Duration.ofMillis(requestTimeout)))
				.build();

		// fileSizeThreshold
		String fileSizeThresholdString = props.getProperty("a2.kinesis.file.size.threshold", "");
		if (fileSizeThresholdString != null && !"".equals(fileSizeThresholdString)) {
			try {
				fileSizeThreshold = Integer.parseInt(fileSizeThresholdString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.kinesis.file.size.threshold -> {}", fileSizeThresholdString);
				LOGGER.warn("Setting it to 512");
			}
		}
	}

}
