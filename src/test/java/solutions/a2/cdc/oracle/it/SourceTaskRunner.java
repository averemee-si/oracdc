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

package solutions.a2.cdc.oracle.it;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * A source connector and its single task started the way a Connect worker starts them, against
 * {@link OracleFree4Mining}, with an offset storage that has nothing: no worker, no broker, the
 * test polls the task itself. Everything runs in the test's thread - the tasks expect
 * {@code stop()} from the thread that polls.
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public final class SourceTaskRunner implements AutoCloseable {

	private final SourceConnector connector;
	private final SourceTask task;
	private final Path tmpDir;

	/**
	 * @param connector   the connector, its task is the one {@link SourceConnector#taskClass()} names
	 * @param name        unique per task in the JVM: the JMX beans and the pools of a task are named after it
	 * @param include     a2.include
	 * @param firstChange a2.first.change, the SCN to start from
	 * @param props       what the test needs beyond the connection, a2.include, a2.first.change and a2.tmpdir
	 */
	public SourceTaskRunner(final SourceConnector connector, final OracleFree4Mining oracle,
			final String name, final String include, final long firstChange,
			final Map<String, String> props) throws Exception {
		this.connector = connector;
		tmpDir = Files.createTempDirectory(name);
		final var config = new HashMap<String, String>();
		config.put("name", name);
		config.put("a2.jdbc.url", oracle.rootJdbcUrl());
		config.put("a2.jdbc.username", OracleFree4Mining.MINER_USER);
		config.put("a2.jdbc.password", OracleFree4Mining.MINER_PASSWORD);
		config.put("a2.include", include);
		config.put("a2.first.change", Long.toString(firstChange));
		config.put("a2.tmpdir", tmpDir.toString());
		config.put("a2.poll.interval", "500");
		config.putAll(props);
		connector.start(config);
		final var taskConfig = connector.taskConfigs(1).get(0);
		task = (SourceTask) connector.taskClass().getDeclaredConstructor().newInstance();
		task.initialize(new EmptyOffsetsContext(taskConfig));
		task.start(taskConfig);
	}

	public SourceTask task() {
		return task;
	}

	/**
	 * Polls the task until {@code expected} records pass {@code filter}, or the timeout: what
	 * {@link SourceTask#poll()} throws, the test gets
	 */
	public List<SourceRecord> poll(final Predicate<SourceRecord> filter, final int expected,
			final long timeoutMs) throws InterruptedException {
		final var polled = new ArrayList<SourceRecord>();
		final long deadline = System.currentTimeMillis() + timeoutMs;
		while (polled.size() < expected && System.currentTimeMillis() < deadline) {
			final List<SourceRecord> records = task.poll();
			if (records != null)
				for (final var record : records)
					if (filter.test(record))
						polled.add(record);
		}
		return polled;
	}

	@Override
	public void close() throws IOException {
		try {
			task.stop();
			connector.stop();
		} finally {
			try (Stream<Path> paths = Files.walk(tmpDir)) {
				paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
			}
		}
	}

	/** The context of a task started for the first time: no committed offset */
	private static final class EmptyOffsetsContext implements SourceTaskContext {

		private final Map<String, String> configs;

		EmptyOffsetsContext(final Map<String, String> configs) {
			this.configs = configs;
		}

		@Override
		public Map<String, String> configs() {
			return configs;
		}

		@Override
		public OffsetStorageReader offsetStorageReader() {
			return new OffsetStorageReader() {
				@Override
				public <T> Map<String, Object> offset(final Map<String, T> partition) {
					return null;
				}
				@Override
				public <T> Map<Map<String, T>, Map<String, Object>> offsets(final Collection<Map<String, T>> partitions) {
					return Map.of();
				}
			};
		}

		@Override
		public PluginMetrics pluginMetrics() {
			return null;
		}

	}

}
