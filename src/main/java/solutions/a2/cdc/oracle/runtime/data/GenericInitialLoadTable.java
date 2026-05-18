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

package solutions.a2.cdc.oracle.runtime.data;

import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.SQLXML;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import oracle.jdbc.OracleBlob;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleClob;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;
import solutions.a2.cdc.oracle.OraCdcColumn;
import solutions.a2.cdc.oracle.OraCdcTableBase;
import solutions.a2.cdc.oracle.runtime.data.GenericAbstractMapDataBinder.KeyValuePair;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class GenericInitialLoadTable {

	private static final Logger LOGGER = LogManager.getLogger(GenericInitialLoadTable.class);
	private static final int LOB_CHUNK_SIZE = 16384;

	private final OraCdcTableBase oraTable;
	private final RowIdStore rowIdStore;
	private final String sqlSelect;

	public GenericInitialLoadTable(boolean parallel, OracleConnection connection,
				OraCdcTableBase oraTable, String whereClause) throws SQLException {
		this.oraTable = oraTable;

		var sbSelectList = new StringBuilder(0x200);
		for (int i = 0; i < oraTable.allColumns().size(); i++) {
			OraCdcColumn oraColumn = oraTable.allColumns().get(i);
			if (Strings.CI.equals(oraColumn.name(),OraCdcColumn.ROWID_KEY))
				sbSelectList
					.append(parallel ? "R.COLUMN_VALUE " : "ROWID ")
					.append(OraCdcColumn.ROWID_KEY);
			else {
				if (Strings.CI.equals(oraColumn.name(), oraColumn.oracleName()))
					sbSelectList.append(oraColumn.name());
				else
					sbSelectList
						.append(oraColumn.oracleName())
						.append(" as ")
						.append(oraColumn.name());
			}
			if (i < oraTable.allColumns().size() - 1)
				sbSelectList.append(",");
		}
		var sbWhereClause = new StringBuilder(0x100);
		var addAndToWhere = false;
		if (!StringUtils.isBlank(whereClause)) {
			sbWhereClause.append((Strings.CI.startsWith(StringUtils.trim(whereClause), "where"))
					? "\n"
					: "\nwhere ");
			sbWhereClause.append(whereClause);
			addAndToWhere = true;
		}
		//TODO
		//TODO Logic also needs improvement!
		//TODO Also support for flashback archive is required!!!
		//TODO
		if (oraTable.rowLevelScn()) {
			if (addAndToWhere)
				sbWhereClause.append(" and ");
			else
				sbWhereClause.append("\nwhere ");
			sbWhereClause.append(" ORA_ROWSCN < ?");
		}
		var sb = new StringBuilder(0x200);
		if (parallel) {
			rowIdStore = new RowIdStoreArrayList();
			var sqlSelectKeys = "select ROWID from " + oraTable.owner() + "." + oraTable.name() + sbWhereClause.toString();
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("'{}' will be used for select keys.", sqlSelectKeys);
			var elapsed = System.currentTimeMillis();
			rowIdStore.readKeys(connection, sqlSelectKeys);
			elapsed = System.currentTimeMillis() - elapsed;
			LOGGER.info("{}.{}: {} rows read in {} milliseconds",
					oraTable.owner(), oraTable.name(), rowIdStore.size(), elapsed);
			sb
				.append("begin")
				.append("\n  open ? for")
				.append("\n  select ")
				.append(sbSelectList)
				.append("\n  from ")
				.append(oraTable.owner())
				.append(".")
				.append(oraTable.name())
				.append(" KU$, (select * from table(?)) R")
				.append("\n  where KU$.ROWID=R.COLUMN_VALUE;")
				.append("\nend;");
		} else {
			rowIdStore = null;
			sb
				.append("select ")
				.append(sbSelectList)
				.append("\nfrom ")
				.append(oraTable.owner())
				.append(".")
				.append(oraTable.name())
				.append(sbWhereClause);

		}
		sqlSelect = sb.toString();
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("{} will be used for initial data load.", sqlSelect);
	}

	int rowCount()  {
		return rowIdStore == null ? 0 : rowIdStore.size();
	}

	Array getRowIdArray(final OracleConnection connection, final int rowNumStart, final int rowNumEnd) throws SQLException {
		return rowIdStore.getRowIdArray(connection, rowNumStart, rowNumEnd);
	}

	OracleCallableStatement prepareSource(final OracleConnection connection) throws SQLException {
		return (OracleCallableStatement) connection.prepareCall(sqlSelect);
	}

	OracleResultSet serialModeRs(OracleConnection connection) throws SQLException {
		var ps = connection.prepareStatement(sqlSelect, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
		if (oraTable.rowLevelScn()) {
			//TODO
			//TODO - SCN!!!
			//TODO
		}
		return (OracleResultSet) ps.executeQuery();
	}

	DataBinder dataBinder(boolean parallel) {
		return parallel
				? (DataBinder) oraTable.dataBinder().newInstance()
				: oraTable.dataBinder();
	}

	KeyValuePair getSourceRecord(OracleResultSet rs, DataBinder dataBinder) throws SQLException {
		if (rs.next()) {
			dataBinder.init();
			try {
				for (var i = 0; i < oraTable.allColumns().size(); i++) {
					var oraColumn = oraTable.allColumns().get(i);
					var oraDatum = rs.getOracleObject(oraColumn.name());
					if (!rs.wasNull()) {
						if (oraColumn.decodeWithoutTrans())
							dataBinder.insert(oraColumn, oraColumn.decoder().decode(oraDatum.getBytes()));
					else {
						switch (oraColumn.jdbcType()) {
						// oracle.sql.[N]CLOB
						case CLOB, NCLOB -> {
							if (((OracleClob)oraDatum).length() > 0) {
								try (var reader = ((OracleClob)oraDatum).getCharacterStream()) {
									if (Integer.MAX_VALUE < ((OracleClob)oraDatum).length()) {
										LOGGER.error(
												"""
												
												=====================
												Unable to process {} column {}({}) with length ({}) greater than Integer.MAX_VALUE ({})
												=====================
												
												""", oraColumn.jdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
												oraTable.fqn(), oraColumn.name(), ((OracleClob)oraDatum).length(), Integer.MAX_VALUE);
										throw new IOException(
												"Unable to process " +
												(oraColumn.jdbcType() == Types.CLOB ? "CLOB" : "NCLOB") +
												"column with length " + ((OracleClob)oraDatum).length() + " chars!");
									}
									var sb = new StringBuilder((int) (((OracleClob)oraDatum).length() * 2));
									var charsRead = 0;
									var data = new char[LOB_CHUNK_SIZE];
									while ((charsRead = reader.read(data, 0, data.length)) != -1)
										sb.append(charsRead == LOB_CHUNK_SIZE
												? data
												: Arrays.copyOfRange(data, 0, charsRead));
									dataBinder.initialLoadSetLob(oraColumn, sb.toString());
									sb = null;
									data = null;
								} catch (IOException ioe) {
									LOGGER.error(
											"""
											
											=====================
											IO Error while processing {} column {}({})
											{}
											=====================
											
											""",  oraColumn.jdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
											oraTable.fqn(), oraColumn.name(), getStackTrace(ioe));
									throw new SQLException(ioe);
								}
							}
						}
						// oracle.sql.BLOB
						case BLOB -> {
							if (((OracleBlob)oraDatum).length() > 0) {
								try (var is = ((OracleBlob)oraDatum).getBinaryStream()) {
									if (Integer.MAX_VALUE < ((OracleBlob)oraDatum).length()) {
										LOGGER.error(
												"""
												
												=====================
												Unable to process BLOB column {}({}) with length ({}) greater than Integer.MAX_VALUE ({})
												=====================
												
												""", oraTable.fqn(), oraColumn.name(), ((OracleBlob)oraDatum).length(), Integer.MAX_VALUE);
										throw new IOException(
												"Unable to process BLOB column with length " + ((OracleBlob)oraDatum).length() + " bytes!");
									}
									var baos = new ByteArrayOutputStream();
									var data = new byte[LOB_CHUNK_SIZE];
									int bytesRead;
									while ((bytesRead = is.read(data, 0, data.length)) != -1)
										baos.write(data, 0, bytesRead);
									dataBinder.initialLoadSetLob(oraColumn, baos.toByteArray());
									data = null;
									baos = null;
								} catch (IOException ioe) {
									LOGGER.error(
											"""
											
											=====================
											IO Error while processing column {}({})
											{}
											=====================
											
											""", oraTable.fqn(), oraColumn.name(), getStackTrace(ioe));
									throw new SQLException(ioe);
								}
							}
						}
						// oracle.xdb.XMLType
						case SQLXML ->
							dataBinder.initialLoadSetLob(oraColumn, ((SQLXML)oraDatum).getString());
						default ->
							throw new SQLException("Unsupported JDBC Type " + oraColumn.jdbcType());
						}
					}
					}
				}
			} catch (SQLException sqle) {
				throw sqle;
			}
			return (KeyValuePair) dataBinder.initialLoadRow();
		} else
			return null;
	}

	String fqn() {
		return oraTable.fqn();
	}

}
