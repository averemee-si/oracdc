/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.utils.ExceptionUtils;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLastProcessedSeqFileNotifier implements LastProcessedSeqNotifier {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcLastProcessedSeqFileNotifier.class);

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
		notify(instant, sequence, null);
	}

	@Override
	public void notify(final Instant instant, final long sequence, final String message) {
		executor.execute(() -> {
			try(PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
				final StringBuilder sb = new StringBuilder(128);
				sb
					.append(sequence)
					.append("\t")
					.append(instant.toString());
				if (StringUtils.isNotBlank(message)) {
					sb
						.append("\t")
						.append(message);
				}
				pw.println(sb.toString());
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
