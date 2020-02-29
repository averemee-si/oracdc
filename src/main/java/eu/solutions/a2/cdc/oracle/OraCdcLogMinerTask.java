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

package eu.solutions.a2.cdc.oracle;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMBeanServer;
import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerTask extends SourceTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerTask.class);

	private int batchSize;
	private int pollInterval;
	private int schemaType;
	private String topic;
	private OraRdbmsInfo rdbmsInfo;
	private OraCdcLogMinerMgmt metrics;
	private OraDumpDecoder odd;
	private Map<Integer, OraTable> tablesInProcessing;
	private BlockingQueue<OraCdcTransaction> committedTransactions;
	private OraCdcLogMinerWorkerThread worker;
	private OraCdcTransaction transaction;
	private boolean lastStatementInTransaction = true;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc logminer source task");

		batchSize = Integer.parseInt(props.get(ParamConstants.BATCH_SIZE_PARAM));
		LOGGER.debug("batchSize = {} records.", batchSize);
		pollInterval = Integer.parseInt(props.get(ParamConstants.POLL_INTERVAL_MS_PARAM));
		LOGGER.debug("pollInterval = {} ms.", pollInterval);
		schemaType = Integer.parseInt(props.get(ParamConstants.SCHEMA_TYPE_PARAM));
		LOGGER.debug("schemaType (Integer value 1 for Debezium, 2 for Kafka STD) = {} .", schemaType);
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			topic = props.get(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM);
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			topic = props.get(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM);
		}
		LOGGER.debug("topic set to {}.", topic);
		

		try {
			rdbmsInfo = OraRdbmsInfo.getInstance();
			metrics = OraCdcLogMinerMBeanServer.getInstance().getMbean();
			odd = new OraDumpDecoder(rdbmsInfo.getDbCharset(), rdbmsInfo.getDbNCharCharset());

			final String sourcePartitionName = rdbmsInfo.getInstanceName() + "_" + rdbmsInfo.getHostName();
			LOGGER.debug("Source Partition {} set to {}.", sourcePartitionName, rdbmsInfo.getDbId());
			final Map<String, String> partition = Collections.singletonMap(sourcePartitionName, ((Long)rdbmsInfo.getDbId()).toString());

			final Long redoSizeThreshold;
			final Integer redoFilesCount;
			if (props.containsKey(ParamConstants.REDO_FILES_SIZE_PARAM)) {
				redoSizeThreshold = Long.parseLong(props.get(ParamConstants.REDO_FILES_SIZE_PARAM));
				redoFilesCount = null;
			} else {
				redoSizeThreshold = null;
				redoFilesCount = Integer.parseInt(props.get(ParamConstants.REDO_FILES_COUNT_PARAM));
			}

			List<String> excludeList = null;
			List<String> includeList = null;
			if (props.containsKey(ParamConstants.TABLE_EXCLUDE_PARAM)) {
				excludeList =
						Arrays.asList(props.get(ParamConstants.TABLE_EXCLUDE_PARAM).split("\\s*,\\s*"));
			}
			if (props.containsKey(ParamConstants.TABLE_INCLUDE_PARAM)) {
				includeList =
						Arrays.asList(props.get(ParamConstants.TABLE_INCLUDE_PARAM).split("\\s*,\\s*"));
			}

			final Path queuesRoot = FileSystems.getDefault().getPath(
					props.get(ParamConstants.TEMP_DIR_PARAM));

			//TODO - sizing of HashMap!!!
			tablesInProcessing = new ConcurrentHashMap<>(64);
			committedTransactions = new LinkedBlockingQueue<>();

			worker = new OraCdcLogMinerWorkerThread(
					pollInterval,
					partition,
					this.context,
					includeList,
					excludeList,
					redoSizeThreshold,
					redoFilesCount,
					tablesInProcessing,
					schemaType,
					topic,
					odd,
					queuesRoot,
					committedTransactions);

		} catch (SQLException | ClassNotFoundException | SecurityException | NoSuchMethodException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		LOGGER.trace("Starting worker thread.");
		worker.start();
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LOGGER.trace("BEGIN: poll()");
		int recordCount = 0;
		int parseTime = 0;
		List<SourceRecord> result = new ArrayList<>();
		while (recordCount < batchSize) {
			if (lastStatementInTransaction) {
				// End of transaction, need to poll new
				transaction = committedTransactions.poll();
				transaction.createTailer();
			}
			if (transaction == null) {
				// No more records produced by LogMiner worker
				break;
			} else {
				// Prepare records...
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Start of processing transaction XID {}, first change {}, commit SCN {}.",
							transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn());
				}
				lastStatementInTransaction = false;
				boolean processTransaction = true;
				do {
					OraCdcLogMinerStatement stmt = new OraCdcLogMinerStatement();
					processTransaction = transaction.getStatement(stmt);
					lastStatementInTransaction = !processTransaction;

					if (processTransaction) {
						final OraTable oraTable = tablesInProcessing.get(stmt.getTableId());
						if (oraTable == null) {
							LOGGER.error("Strange consistency issue for DATA_OBJ# {}. Exiting.", stmt.getTableId());
							throw new ConnectException("Strange consistency issue!!!");
						} else {
							try {

								final long startParseTs = System.currentTimeMillis();
								SourceRecord record = oraTable.parseRedoRecord(stmt);
								result.add(record);
								recordCount++;
								parseTime += (System.currentTimeMillis() - startParseTs);
							} catch (SQLException e) {
								LOGGER.error(e.getMessage());
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
								throw new ConnectException(e);
							}
						}
					}
				} while (processTransaction && recordCount < batchSize);
				if (lastStatementInTransaction) {
					// close Cronicle queue only when all statements are processed
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("End of processing transaction XID {}, first change {}, commit SCN {}.",
								transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn());
					}
					transaction.close();
					transaction = null;
				}
			}
		}
		if (recordCount == 0) {
			synchronized (this) {
				LOGGER.trace("Waiting {} ms", pollInterval);
				this.wait(pollInterval);
			}
		} else {
			metrics.addSentRecords(result.size(), parseTime);
		}
		LOGGER.trace("END: poll()");
		return result;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping oracdc logminer source task.");
		worker.shutdown();
		if (transaction != null) {
			LOGGER.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			LOGGER.error("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			LOGGER.error("Unprocessed committed transaction XID={} for processing!!!", transaction.getXid());
			LOGGER.error("\tUnprocessed committed transaction XID {}, first change {}, commit SCN {}, number of rows {}.",
					transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn(), transaction.length());
			LOGGER.error("Please check information below and set a2.first.change to appropriate value!!!");
			LOGGER.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			//TODO
			//TODO persistence???
			//TODO
			transaction.close();
			throw new ConnectException("Unprocessed transactions left!!!\n" + 
					"Please check connector log files!!!");
		}
	}

}