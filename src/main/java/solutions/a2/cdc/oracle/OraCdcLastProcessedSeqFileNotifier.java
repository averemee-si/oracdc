/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
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

package solutions.a2.cdc.oracle;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.utils.ExceptionUtils;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLastProcessedSeqFileNotifier implements LastProcessedSeqNotifier {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLastProcessedSeqFileNotifier.class);

	private String fileName;
	private ExecutorService executor;

	@Override
	public void configure(OraCdcSourceConnectorConfig config) {
		fileName = config.getLastProcessedSeqNotifierFile();
		executor = Executors.newSingleThreadExecutor();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			executor.shutdown();
			while (true) {
				LOGGER.info("Waiting for background FileWriter to shutdown");
				try {
					if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
						break;
					}
				} catch (InterruptedException ie) {}
			}
		}));
	}

	@Override
	public void notify(final Instant instant, final long sequence) {
		executor.execute(() -> {
			try(PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
				pw.println(sequence + "\t" + instant.toString());
			} catch (IOException ioe) {
				LOGGER.error(
						"\n=====================\n" +
						"'{}' while writingsequence '{}' to '{}'.\n" +
						ExceptionUtils.getExceptionStackTrace(ioe) +
						"\n" +
						"=====================\n",
						ioe.getMessage(), sequence, fileName);
			}
		});
	}

}
