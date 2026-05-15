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

import static java.sql.Types.BOOLEAN;
import static java.sql.Types.TINYINT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.BIGINT;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.DATE;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.CHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.ROWID;
import static java.sql.Types.SQLXML;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.LONGNVARCHAR;
import static oracle.jdbc.OracleTypes.BINARY_DOUBLE;
import static oracle.jdbc.OracleTypes.BINARY_FLOAT;
import static oracle.jdbc.OracleTypes.INTERVALDS;
import static oracle.jdbc.OracleTypes.INTERVALYM;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.cdc.Column.JAVA_SQL_TYPE_INTERVALDS_STRING;
import static solutions.a2.cdc.Column.JAVA_SQL_TYPE_INTERVALYM_STRING;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_TOLERATE_INCOMPLETE_ROW;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import oracle.sql.json.OracleJsonFactory;
import solutions.a2.cdc.oracle.OraCdcColumn;
import solutions.a2.cdc.oracle.OraCdcDataException;
import solutions.a2.cdc.oracle.OraCdcDecoder;
import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraCdcTableBase;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraRdbmsInfo;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class GenericMapDataBinder extends GenericAbstractMapDataBinder {

	private static final Logger LOGGER = LogManager.getLogger(GenericMapDataBinder.class);

	private int mandatoryColumnsProcessed = 0;

	public GenericMapDataBinder(final OraRdbmsInfo rdbmsInfo, final OraCdcTableBase table) {
		super(rdbmsInfo, table);
	}

	public void init(OraCdcStatementBase stmt) {
		super.init(stmt);
		mandatoryColumnsProcessed = 0;
	}

	@Override
	public void insert(OraCdcColumn column, Object value) {
		if (column.partOfPk()) {
			keyData.put(column.getColumnId(), value);
			mandatoryColumnsProcessed++;
		} else {
			if (column.jdbcType() == BLOB ||
							column.jdbcType() == CLOB ||
							column.jdbcType() == NCLOB ||
							column.jdbcType() == SQLXML ||
							column.jdbcType() == JSON ||
							column.jdbcType() == VECTOR)
				valueData.put(column.getColumnId(), Optional.ofNullable(value));
			else
				valueData.put(column.getColumnId(), value);
			if (!column.nullable())
				mandatoryColumnsProcessed++;
		}
	}

	@Override
	public void delete(OraCdcColumn column, Object value) {
		if (column.partOfPk()) {
			keyData.put(column.getColumnId(), value);
			mandatoryColumnsProcessed++;
		} else {
			if (column.jdbcType() == BLOB ||
					column.jdbcType() == CLOB ||
					column.jdbcType() == NCLOB ||
					column.jdbcType() == SQLXML ||
					column.jdbcType() == JSON ||
					column.jdbcType() == VECTOR)
				valueData.put(column.getColumnId(), Optional.ofNullable(value));
			else
				valueData.put(column.getColumnId(), value);
			if (!column.nullable())
				mandatoryColumnsProcessed++;
		}
	}

	@Override
	public void update(OraCdcColumn column, Object value, boolean after) {
		if (column.partOfPk()) {
			keyData.put(column.getColumnId(), value);
			mandatoryColumnsProcessed++;
		} else {
			if (column.jdbcType() == BLOB ||
					column.jdbcType() == CLOB ||
					column.jdbcType() == NCLOB ||
					column.jdbcType() == SQLXML ||
					column.jdbcType() == JSON ||
					column.jdbcType() == VECTOR)
				valueData.put(column.getColumnId(), Optional.ofNullable(value));
			else
				valueData.put(column.getColumnId(), value);
			if (!column.nullable())
				mandatoryColumnsProcessed++;
		}
	}

	@Override
	public void buildSchema(boolean initial) throws SQLException {
		for (var column : table.allColumns()) {
			final OraCdcDecoder decoder; 
			switch (column.jdbcType()) {
				case CHAR, VARCHAR, NCHAR, NVARCHAR -> {
					var charset = rdbmsInfo == null
							? "US7ASCII"
							: column.jdbcType() == CHAR || column.jdbcType() == VARCHAR
									? rdbmsInfo.charset()
									: rdbmsInfo.nCharset();
					decoder = column.encrypted()
							? GenericDecoders.get(charset, table.decrypter(), column.salted())
							: GenericDecoders.get(charset);
				}
				case LONGVARBINARY, LONGVARCHAR, LONGNVARCHAR,
						TINYINT, SMALLINT, INTEGER, BIGINT, BOOLEAN,
						DATE, TIMESTAMP, JAVA_SQL_TYPE_INTERVALYM_STRING, JAVA_SQL_TYPE_INTERVALDS_STRING,
						BLOB, CLOB, NCLOB, SQLXML, VECTOR -> 
					decoder = decoderFromJdbcType(column);
				case BINARY, NUMERIC, INTERVALYM, INTERVALDS, ROWID ->
					decoder = column.encrypted()
							? GenericDecoders.get(table.decrypter(), column.salted())
							: GenericDecoders.get();
				case FLOAT -> 
					decoder = column.IEEE754()
							? column.encrypted()
									? GenericDecoders.get(BINARY_FLOAT, table.decrypter(), column.salted())
									: GenericDecoders.get(BINARY_FLOAT)
							: decoderFromJdbcType(column);
				case DOUBLE -> 
					decoder = column.IEEE754()
							? column.encrypted()
									? GenericDecoders.get(BINARY_DOUBLE, table.decrypter(), column.salted())
									: GenericDecoders.get(BINARY_DOUBLE)
							: decoderFromJdbcType(column);
				case DECIMAL ->
					decoder = column.encrypted()
							? GenericDecoders.getNUMBER(column.dataScale(), table.decrypter(), column.salted())
							: GenericDecoders.getNUMBER(column.dataScale());
				case TIMESTAMP_WITH_TIMEZONE ->
					decoder = column.encrypted()
							? GenericDecoders.get(
									rdbmsInfo == null ? ZoneId.systemDefault() : rdbmsInfo.getDbTimeZone(),
									column.localTimeZone(), table.decrypter(), column.salted())
							: GenericDecoders.get(
									rdbmsInfo == null ? ZoneId.systemDefault() : rdbmsInfo.getDbTimeZone(),
									column.localTimeZone());
				case JSON ->
					decoder = column.encrypted()
							? GenericDecoders.get(
									rdbmsInfo == null ? new OracleJsonFactory() : rdbmsInfo.jsonFactory(),
									table.decrypter(), column.salted())
							: GenericDecoders.get(
									rdbmsInfo == null ? new OracleJsonFactory() : rdbmsInfo.jsonFactory());
				default ->
					decoder = null;
			
			}
			column.decoder(decoder);
		}
	}

	@Override
	public KeyValuePair changeVector(OraCdcTransaction transaction, Map<String, Object> offset, boolean skipRedoRecord) throws SQLException {
		if (skipRedoRecord)
			return null;
		else {
			if (stmt.getOperation() != DELETE) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug(
							"Mandatory columns count for table {} is {}, but only {} mandatory columns are returned from redo record!",
							table.fqn(), table.mandatoryColumnsCount(), mandatoryColumnsProcessed);

				if ((table.flags() & FLG_TOLERATE_INCOMPLETE_ROW) > 0) {
					LOGGER.error(TOLERANCE_ERR_MSG, table.fqn(), table.mandatoryColumnsCount(),
							mandatoryColumnsProcessed,
							Long.toUnsignedString(stmt.getScn()), stmt.getRba(),
							Long.toUnsignedString(transaction.getCommitScn()), transaction.getXid(),
							stmt.getSqlRedo(), "Skipping!");
					return null;
				} else {
					LOGGER.error(TOLERANCE_ERR_MSG, table.fqn(), table.mandatoryColumnsCount(),
							mandatoryColumnsProcessed,
							Long.toUnsignedString(stmt.getScn()), stmt.getRba(),
							Long.toUnsignedString(transaction.getCommitScn()), transaction.getXid(),
							stmt.getSqlRedo(), "Exiting!");
					throw new OraCdcDataException("Incomplete redo record!");
				}
			}
			return new KeyValuePair(stmt.getOperation(), keyData, valueData);
		}		
	}

	@Override
	public DataBinder newInstance() {
		return new GenericMapDataBinder(this.rdbmsInfo, this.table);
	}

	private OraCdcDecoder decoderFromJdbcType(final OraCdcColumn column) {
		return column.encrypted()
				? GenericDecoders.get(column.jdbcType(), table.decrypter(), column.salted())
				: GenericDecoders.get(column.jdbcType());
	}

}
