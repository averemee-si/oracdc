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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import solutions.a2.cdc.oracle.it.OracleFree;
import solutions.a2.cdc.oracle.it.OracleFree4Mining;
import solutions.a2.cdc.oracle.it.SourceTaskRunner;

/**
 * The initial load of {@link OraCdcRedoMinerConnector} ({@code a2.initial.load=EXECUTE}) against
 * Oracle Free in ARCHIVELOG mode: every row of SCOTT.EMP has to come out of
 * {@code SourceTask.poll()} as a valid record of the schema type the connector is set to.
 * <p>
 * The settings are those of <a href="https://github.com/averemee-si/oracdc/issues/200">issue 200</a>
 * - RedoMiner, {@code a2.pk.type=well_defined}, {@code a2.use.rowid.as.key=true},
 * {@code a2.supplemental.logging=ALL}, {@code a2.process.online.redo.logs=true},
 * {@code a2.topic.name.style=SCHEMA_TABLE} - with the
 * redo logs read as BFILE through the directory objects of {@link OracleFree4Mining} instead of SSH.
 * The issue: with {@code a2.schema.type=debezium} the send to Kafka phase fails on the first row
 * with {@code DataException: Struct schemas do not match.}; with the default schema type it does not.
 * <p>
 * {@code ./mvnw -Pdocker -Dgpg.skip verify -Dit.test=OraCdcRedoMinerInitialLoadIT}; the image is
 * {@code -Dit.oracle.image}.
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
@TestInstance(PER_CLASS)
public class OraCdcRedoMinerInitialLoadIT {

	private static final long POLL_TIMEOUT_MS = 180_000;
	private static final String TABLE = "EMP";

	private final OracleFree4Mining oracle = new OracleFree4Mining();
	/**
	 * The SCN the CDC after the initial load starts from: {@code task.start()} returns only when
	 * the RedoMiner has a log with it, so it is in an archived log before any task starts
	 */
	private long firstChange;

	@BeforeAll
	void start() throws Exception {
		oracle.start();
		firstChange = oracle.currentScn();
		oracle.archiveLogCurrent();
	}

	@AfterAll
	void stop() {
		oracle.close();
	}

	/** Issue 200: the Debezium envelope of every row, op "c", no "before", the row in "after" */
	@Test
	void testDebeziumInitialLoad() throws Exception {
		assertDebeziumInitialLoad("il-debezium-it", Map.of());
	}

	/**
	 * Issue 200 with all the columns of a DELETE: the value schema of the other schema types is
	 * then required, in the envelope it has to stay optional for the "before" of every row
	 */
	@Test
	void testDebeziumInitialLoadAllColumnsOnDelete() throws Exception {
		assertDebeziumInitialLoad("il-debezium-all-cols-it", Map.of("a2.use.all.columns.on.delete", "true"));
	}

	private void assertDebeziumInitialLoad(final String name, final Map<String, String> extra) throws Exception {
		try (var runner = runner(name, "debezium", extra)) {
			final var records = runner.poll(OraCdcRedoMinerInitialLoadIT::isEmpDebezium, OracleFree.EMP_ROWS, POLL_TIMEOUT_MS);
			assertEquals(OracleFree.EMP_ROWS, records.size(), "Initial load records of SCOTT.EMP: " + records);
			final Set<Integer> empNos = new TreeSet<>();
			for (final var record : records) {
				final var envelope = (Struct) record.value();
				envelope.validate();
				assertEquals(record.valueSchema(), envelope.schema());
				assertEquals("c", envelope.getString("op"));
				final var source = envelope.getStruct("source");
				assertNotNull(source, "Envelope without source: " + envelope);
				assertEquals(OracleFree.USER, source.getString("owner"));
				// a row the initial load reads has no previous image, not even its key
				assertNull(envelope.get("before"), "Envelope with before: " + envelope);
				// the key goes to the key of the record, as for the changes from the redo
				final var key = (Struct) record.key();
				assertNotNull(key, "Record without key: " + record);
				key.validate();
				final var after = envelope.getStruct("after");
				assertNotNull(after, "Envelope without after: " + envelope);
				assertEquals(empNo(key), empNo(after));
				empNos.add(empNo(after));
				assertNotNull(after.get("ENAME"), "Row without ENAME: " + after);
			}
			assertEquals(OracleFree.EMP_ROWS, empNos.size(), "EMPNO of the rows: " + empNos);
		}
	}

	/** The control: with the default schema type, key and value of every row, header op=c */
	@Test
	void testKafkaInitialLoad() throws Exception {
		try (var runner = runner("il-kafka-it", "kafka", Map.of())) {
			final var records = runner.poll(OraCdcRedoMinerInitialLoadIT::isEmpKafka, OracleFree.EMP_ROWS, POLL_TIMEOUT_MS);
			assertEquals(OracleFree.EMP_ROWS, records.size(), "Initial load records of SCOTT.EMP: " + records);
			final Set<Integer> empNos = new TreeSet<>();
			for (final var record : records) {
				assertEquals("c", record.headers().lastWithName("op").value());
				final var key = (Struct) record.key();
				assertNotNull(key, "Record without key: " + record);
				key.validate();
				// the key column is in the key only
				empNos.add(empNo(key));
				final var value = (Struct) record.value();
				value.validate();
				assertNotNull(value.get("ENAME"), "Row without ENAME: " + value);
			}
			assertEquals(OracleFree.EMP_ROWS, empNos.size(), "EMPNO of the rows: " + empNos);
		}
	}

	private SourceTaskRunner runner(final String name, final String schemaType,
			final Map<String, String> extra) throws Exception {
		final var props = new HashMap<String, String>(Map.of(
				"a2.storage.media", "BFILE",
				"a2.bfile.directory.online", OracleFree4Mining.ONLINE_DIR,
				"a2.bfile.directory.archive", OracleFree4Mining.ARCHIVE_DIR,
				"a2.initial.load", "EXECUTE",
				"a2.pk.type", "well_defined",
				"a2.use.rowid.as.key", "true",
				"a2.supplemental.logging", "ALL",
				"a2.process.online.redo.logs", "true",
				"a2.topic.name.style", "SCHEMA_TABLE",
				"a2.topic.name.delimiter", "."));
		props.put("a2.topic.prefix", "oraredocdc.");
		props.put("a2.schema.type", schemaType);
		props.putAll(extra);
		return new SourceTaskRunner(new OraCdcRedoMinerConnector(), oracle, name,
				OracleFree.USER + "." + TABLE, firstChange, props);
	}

	private static boolean isEmpDebezium(final SourceRecord record) {
		return record.value() instanceof Struct envelope &&
				envelope.schema().field("source") != null &&
				envelope.getStruct("source") != null &&
				TABLE.equals(envelope.getStruct("source").getString("table"));
	}

	private static boolean isEmpKafka(final SourceRecord record) {
		return record.topic() != null && record.topic().endsWith("." + TABLE) &&
				record.value() instanceof Struct;
	}

	private static int empNo(final Struct row) {
		final var value = row.get("EMPNO");
		assertTrue(value instanceof Number, "EMPNO " + value + " in " + row);
		return ((Number) value).intValue();
	}

}
