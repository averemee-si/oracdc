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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.concurrent.CountDownLatch;

import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.jmx.OraCdcSourceConnMgmt;

import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_2396;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_17002;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_17008;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_17410;

/**
 * 
 * Dictionary check utilities
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraCdcDictionaryChecker {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcDictionaryChecker.class);
	private static final int MAX_RETRIES = 63;

	private final OraCdcTaskBase task;
	private final boolean staticObjIds;
	private final OraConnectionObjects oraConnections;
	private final OraCdcSourceConnectorConfig config;
	private final OraRdbmsInfo rdbmsInfo;
	private final Long2ObjectHashMap<OraCdcTableBase> tablesInProcessing;
	private final Long2LongHashMap partitionsInProcessing;
	private final LongHashSet tablesOutOfScope;
	private final String checkTableSql;
	private final CountDownLatch runLatch;
	private final boolean isCdb;
	private final int connectionRetryBackoff;
	private final IntHashSet includeObjIds;
	private final boolean includeFilter;
	private final IntHashSet excludeObjIds;
	private final boolean excludeFilter;
	private final OraCdcSourceConnMgmt metrics;
	private Connection connection;
	private PreparedStatement psCheckTable;
	private boolean logMiner;

	public OraCdcDictionaryChecker(
			final OraCdcTaskBase task,
			final Long2ObjectHashMap<OraCdcTableBase> tablesInProcessing,
			final LongHashSet tablesOutOfScope,
			final String checkTableSql,
			final OraCdcSourceConnMgmt metrics) throws SQLException {
		this(task, false, tablesInProcessing, tablesOutOfScope, checkTableSql, null, null, metrics);
	}

	public OraCdcDictionaryChecker(
			final OraCdcTaskBase task,
			final boolean staticObjIds,
			final Long2ObjectHashMap<OraCdcTableBase> tablesInProcessing,
			final LongHashSet tablesOutOfScope,
			final String checkTableSql,
			IntHashSet includeObjIds,
			IntHashSet excludeObjIds,
			final OraCdcSourceConnMgmt metrics) throws SQLException {
		this.task = task;
		this.staticObjIds = staticObjIds;
		this.includeObjIds = includeObjIds;
		if (includeObjIds == null || includeObjIds.size() == 0)
			includeFilter = false;
		else
			includeFilter = true;
		this.excludeObjIds = excludeObjIds;
		if (excludeObjIds == null || excludeObjIds.size() == 0)
			excludeFilter = false;
		else
			excludeFilter = true;
		this.oraConnections = task.oraConnections();
		this.config = task.config();
		this.rdbmsInfo = task.rdbmsInfo();
		this.runLatch = task.runLatch();
		this.tablesInProcessing = tablesInProcessing;
		this.tablesOutOfScope = tablesOutOfScope;
		this.checkTableSql = checkTableSql;
		this.isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		this.connectionRetryBackoff = config.connectionRetryBackoff();
		this.connection = oraConnections.getConnection();
		this.metrics = metrics;
		this.partitionsInProcessing = new Long2LongHashMap(config.tablesInProcessSize(), .7f, 0L);
		logMiner = config.logMiner();
		initStatements();
	}

	public OraCdcTableBase getTable(final long combinedDataObjectId) throws SQLException {
		return getTable(
				combinedDataObjectId,
				(int) combinedDataObjectId,
				(combinedDataObjectId >> 32)  & 0xFFFFFFFFL);
	}

	OraCdcTableBase getTable(long combinedDataObjectId, final long dataObjectId, final long conId) throws SQLException {
		OraCdcTableBase oraTable = tablesInProcessing.get(combinedDataObjectId);
		if (oraTable == null && !tablesOutOfScope.contains(combinedDataObjectId)) {
			var combinedParentTableId = partitionsInProcessing.get(combinedDataObjectId);
			if (combinedParentTableId != 0L) {
				return tablesInProcessing.get(combinedParentTableId);
			} else {
				// Check for object...
				ResultSet rsCheckTable = null;
				boolean wait4CheckTableCursor = true;
				while (runLatch.getCount() > 0 && wait4CheckTableCursor) {
					try {
						psCheckTable.setLong(1, dataObjectId);
						if (isCdb) {
							psCheckTable.setLong(2, conId);
						}
						rsCheckTable = psCheckTable.executeQuery();
						wait4CheckTableCursor = false;
						break;
					} catch (SQLException sqle) {
						if (sqle.getErrorCode() == ORA_2396 ||
								sqle.getErrorCode() == ORA_17002 ||
								sqle.getErrorCode() == ORA_17008 ||
								sqle.getErrorCode() == ORA_17410 || 
								sqle instanceof SQLRecoverableException ||
								(sqle.getCause() != null && sqle.getCause() instanceof SQLRecoverableException)) {
							LOGGER.warn(
									"\n=====================\n" +
									"Encontered an 'ORA-{}: {}'\n" +
									"Attempting to reconnect to dictionary...\n" +
									"=====================\n",
									sqle.getErrorCode(), sqle.getMessage());
							try {
								try {
									connection.close();
									connection = null;
								} catch(SQLException unimportant) {
									LOGGER.warn(
											"\n=====================\n" +
											"Unable to close inactive dictionary connection after 'ORA-{}'\n" +
											"=====================\n",
											sqle.getErrorCode());
								}
								boolean ready = false;
								int retries = 0;
								while (runLatch.getCount() > 0 && !ready) {
									try {
										connection = oraConnections.getConnection();
										initStatements();
									} catch(SQLException sqleRestore) {
										if (retries > MAX_RETRIES) {
											LOGGER.error(
													"\n=====================\n" +
													"Unable to restore dictionary connection after {} retries!\n" +
													"=====================\n",
													retries);
											throw sqleRestore;
										}
									}
									ready = true;
									if (!ready) {
										long waitTime = (long) Math.pow(2, retries++) + connectionRetryBackoff;
										LOGGER.warn("Waiting {} ms for dictionary connection to restore...", waitTime);
										try {
											this.wait(waitTime);
										} catch (InterruptedException ie) {}
									}
								}
							} catch (SQLException ucpe) {
								LOGGER.error(
										"\n=====================\n" +
										"SQL errorCode = {}, SQL state = '{}' while restarting connection to dictionary tables\n" +
										"SQL error message = {}\n" +
										"=====================\n",
										ucpe.getErrorCode(), ucpe.getSQLState(), ucpe.getMessage());
								throw new SQLException(sqle);
							}
						} else {
							LOGGER.error(
									"\n=====================\n" +
									"SQL errorCode = {}, SQL state = '{}' while trying to SELECT from dictionary tables\n" +
									"SQL error message = {}\n" +
									"=====================\n",
									sqle.getErrorCode(), sqle.getSQLState(), sqle.getMessage());
							throw new SQLException(sqle);
						}
					}
				}
				if (rsCheckTable.next()) {
					//May be this is partition, so just check tablesInProcessing map for table
					boolean needNewTableDefinition = true;
					final boolean isPartition = Strings.CS.equals("N", rsCheckTable.getString("IS_TABLE"));
					if (isPartition) {
						final long parentTableId = rsCheckTable.getLong("PARENT_OBJECT_ID");
						combinedParentTableId = isCdb ?
								((conId << 32) | (parentTableId & 0xFFFFFFFFL)) :
								parentTableId;
						oraTable = tablesInProcessing.get(combinedParentTableId);
						if (oraTable != null) {
							needNewTableDefinition = false;
							partitionsInProcessing.put(combinedDataObjectId, combinedParentTableId);
							metrics.addPartitionInProcessing();
							combinedDataObjectId = combinedParentTableId;
						}
					}
					//Get table definition from RDBMS
					if (needNewTableDefinition) {
						final String tableName = rsCheckTable.getString("TABLE_NAME");
						final String tableOwner = rsCheckTable.getString("OWNER");
						if (logMiner)
							oraTable = new OraCdcLogMinerTable(
									isCdb ? rsCheckTable.getString("PDB_NAME") : null,
									isCdb ? (short) conId : -1,
									tableOwner, tableName,
									"ENABLED".equalsIgnoreCase(rsCheckTable.getString("DEPENDENCIES")),
									config, rdbmsInfo, connection, task.getTableVersion(combinedDataObjectId));
						else
							oraTable = new OraCdcRedoMinerTable(
									isCdb ? rsCheckTable.getString("PDB_NAME") : null,
									isCdb ? (short) conId : -1,
									tableOwner, tableName,
									"ENABLED".equalsIgnoreCase(rsCheckTable.getString("DEPENDENCIES")),
									config, rdbmsInfo, connection, task.getTableVersion(combinedDataObjectId));
						task.putTableVersion(combinedDataObjectId, 1);

						if (isPartition) {
							partitionsInProcessing.put(combinedDataObjectId, combinedParentTableId);
							metrics.addPartitionInProcessing();
							combinedDataObjectId = combinedParentTableId;
						}
						tablesInProcessing.put(combinedDataObjectId, oraTable);
						metrics.addTableInProcessing(oraTable.fqn());
					}
				} else {
					tablesOutOfScope.add(combinedDataObjectId);
					metrics.addTableOutOfScope();
				}
				rsCheckTable.close();
				rsCheckTable = null;
				psCheckTable.clearParameters();
			}
		}
		return oraTable;
	}

	boolean containsTable(final long combinedDataObjectId) {
		return tablesInProcessing.containsKey(combinedDataObjectId); 
	}

	void initStatements() throws SQLException {
		psCheckTable = connection.prepareStatement(
				checkTableSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	public void printConsistencyError(final OraCdcTransaction transaction, final OraCdcStatementBase stmt) {
		final StringBuilder sb = new StringBuilder(0x800);
		sb
			.append("\n=====================\n")
			.append("Strange consistence issue: DATA_OBJ# ")
			.append((int)stmt.getTableId())
			.append(" (combined_id=")
			.append(stmt.getTableId())
			.append(") is missed in connector dictionary.\n")
			.append("Transaction details: XID=")
			.append(transaction.getXid())
			.append(", SCN=")
			.append(stmt.getScn())
			.append(", RBA=")
			.append(stmt.getRba())
			.append(", SUBSCN=")
			.append(stmt.getSsn())
			.append("\nThe connector dictionary contains only definitions of the following objects:");
		tablesInProcessing.keySet().forEach(id ->
			sb
				.append("\n\t")
				.append(tablesInProcessing.get(id).fqn())
				.append("\tOBJ_ID=")
				.append((int)id.longValue())
				.append(" (")
				.append(id)
				.append(")"));
		sb.append("\n=====================\n");
		LOGGER.error(sb.toString());
	}

	public boolean notNeeded(final int obj, final short conId) throws SQLException {
		if (staticObjIds) {
			if ((includeFilter &&
					!includeObjIds.contains(obj)) ||
				(excludeFilter &&
						excludeObjIds.contains(obj))) {
				return true;
			} else {
				return false;
			}
		} else {
			//TODO - dynamic LOB/IOT OBJECT_ID's support
			final long combinedDataObjectId = isCdb ?
					(((long)conId) << 32) | ((long)obj & 0xFFFFFFFFL) :
					obj;
			if (tablesOutOfScope.contains(combinedDataObjectId)) {
				return true;
			} else {
				if (tablesInProcessing.containsKey(combinedDataObjectId)) {
					return false;
				} else {
					if (partitionsInProcessing.containsKey(combinedDataObjectId)) {
						return false;
					} else {
						if (getTable(combinedDataObjectId, obj, conId) == null) {
							return true;
						} else {
							return false;
						}
					}
				}
			}
		}
	}
}
