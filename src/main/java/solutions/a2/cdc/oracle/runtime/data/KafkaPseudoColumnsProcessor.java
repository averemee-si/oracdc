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

package solutions.a2.cdc.oracle.runtime.data;

import static org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INT64_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_STRING_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_UNIXTIME_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_INT64_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_STRING_SCHEMA;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import solutions.a2.cdc.oracle.OraCdcPseudoColumnsProcessor;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraCdcTransaction;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class KafkaPseudoColumnsProcessor implements OraCdcPseudoColumnsProcessor {

	private final boolean addRowScn;
	private final String rowScnName;
	private final boolean addRowTs;
	private final String rowTsName;
	private final boolean addCommitScn;
	private final String commitScnName;
	private final boolean addRowOp;
	private final String rowOpName;
	private final boolean addRowXid;
	private final String rowXidName;

	private final boolean addUsernameField;
	private final String usernameField;
	private final boolean addOsUsernameField;
	private final String osUsernameField;
	private final boolean addHostnameField;
	private final String hostnameField;
	private final boolean addAuditSessionIdField;
	private final String auditSessionIdField;
	private final boolean addSessionInfoField;
	private final String sessionInfoField;
	private final boolean addClientIdField;
	private final String clientIdField;

	private final boolean auditNeeded;
	
	private interface PseudoColumnsHandler {
		void addToSchema(final SchemaBuilder builder);
		void addToStruct(final Struct struct, final OraCdcStatementBase stmt, final OraCdcTransaction transaction);
	}

	private final PseudoColumnsHandler handler;

	public KafkaPseudoColumnsProcessor(final OraCdcSourceConnectorConfig config) {
		rowScnName = config.getOraRowScnField();
		addRowScn = StringUtils.isNotBlank(rowScnName);
		commitScnName = config.getOraCommitScnField();
		addCommitScn = StringUtils.isNotBlank(commitScnName);
		rowTsName = config.getOraRowTsField();
		addRowTs = StringUtils.isNotBlank(rowTsName);
		rowOpName = config.getOraRowOpField();
		addRowOp = StringUtils.isNotBlank(rowOpName);
		rowXidName = config.getOraXidField();
		addRowXid = StringUtils.isNotBlank(rowXidName);

		usernameField = config.getOraUsernameField();
		addUsernameField = StringUtils.isNotBlank(usernameField);
		osUsernameField = config.getOraOsUsernameField();
		addOsUsernameField = StringUtils.isNotBlank(osUsernameField);
		hostnameField = config.getOraHostnameField();
		addHostnameField = StringUtils.isNotBlank(hostnameField);
		auditSessionIdField = config.getOraAuditSessionIdField();
		addAuditSessionIdField = StringUtils.isNotBlank(auditSessionIdField);
		sessionInfoField = config.getOraSessionInfoField();
		addSessionInfoField = StringUtils.isNotBlank(sessionInfoField);
		clientIdField = config.getOraClientIdField();
		addClientIdField = StringUtils.isNotBlank(clientIdField);

		auditNeeded =
				addUsernameField || addOsUsernameField || addHostnameField ||
				addAuditSessionIdField || addSessionInfoField || addClientIdField;

		if (addRowScn || addCommitScn || addRowTs || addRowOp || auditNeeded) {
			if (config.supplementalLogAll())
				handler = new PseudoColumnsHandler() {
					@Override
					public void addToSchema(SchemaBuilder builder) {
						if (addRowScn)
							builder.field(rowScnName, INT64_SCHEMA);
						if (addRowTs)
							builder.field(rowTsName, Timestamp.builder().required().build());
						if (addCommitScn)
							builder.field(commitScnName, INT64_SCHEMA);
						if (addRowOp)
							builder.field(rowOpName, STRING_SCHEMA);
						if (addRowXid)
							builder.field(rowXidName, STRING_SCHEMA);

						if (auditNeeded) {
							if (addUsernameField)
								builder.field(usernameField, OPTIONAL_STRING_SCHEMA);
							if (addOsUsernameField)
								builder.field(osUsernameField, OPTIONAL_STRING_SCHEMA);
							if (addHostnameField)
								builder.field(hostnameField, OPTIONAL_STRING_SCHEMA);
							if (addAuditSessionIdField)
								builder.field(auditSessionIdField, OPTIONAL_INT64_SCHEMA);
							if (addSessionInfoField)
								builder.field(sessionInfoField, OPTIONAL_STRING_SCHEMA);
							if (addClientIdField)
								builder.field(clientIdField, OPTIONAL_STRING_SCHEMA);
						}
					}
					@Override
					public void addToStruct(Struct struct, OraCdcStatementBase stmt, OraCdcTransaction transaction) {
						if (addRowScn)
							struct.put(rowScnName, stmt.getScn());
						if (addRowTs)
							struct.put(rowTsName, stmt.getTimestamp());
						if (addCommitScn)
							struct.put(commitScnName, transaction.getCommitScn());
						if (addRowOp)
							struct.put(rowOpName, stmt.opName());
						if (addRowXid)
							struct.put(rowXidName, transaction.getXid());
						if (auditNeeded) {
							if (addUsernameField)
								struct.put(usernameField, transaction.getUsername());
							if (addOsUsernameField)
								struct.put(osUsernameField, transaction.getOsUsername());
							if (addHostnameField)
								struct.put(hostnameField, transaction.getHostname());
							if (addAuditSessionIdField)
								struct.put(auditSessionIdField, transaction.getAuditSessionId());
							if (addSessionInfoField)
								struct.put(sessionInfoField, transaction.getSessionInfo());
							if (addClientIdField)
								struct.put(clientIdField, transaction.getClientId());
						}
					}
			};
			else
				handler = new PseudoColumnsHandler() {
				@Override
				public void addToSchema(SchemaBuilder builder) {
					if (addRowScn)
						builder.field(rowScnName, WRAPPED_INT64_SCHEMA);
					if (addRowTs)
						builder.field(rowTsName, WRAPPED_UNIXTIME_SCHEMA);
					if (addCommitScn)
						builder.field(commitScnName, WRAPPED_INT64_SCHEMA);
					if (addRowOp)
						builder.field(rowOpName, WRAPPED_STRING_SCHEMA);
					if (addRowXid)
						builder.field(rowXidName, WRAPPED_STRING_SCHEMA);

					if (auditNeeded) {
						if (addUsernameField)
							builder.field(usernameField, WRAPPED_OPT_STRING_SCHEMA);
						if (addOsUsernameField)
							builder.field(osUsernameField, WRAPPED_OPT_STRING_SCHEMA);
						if (addHostnameField)
							builder.field(hostnameField, WRAPPED_OPT_STRING_SCHEMA);
						if (addAuditSessionIdField)
							builder.field(auditSessionIdField, WRAPPED_OPT_INT64_SCHEMA);
						if (addSessionInfoField)
							builder.field(sessionInfoField, WRAPPED_OPT_STRING_SCHEMA);
						if (addClientIdField)
							builder.field(clientIdField, WRAPPED_OPT_STRING_SCHEMA);
					}
				}
				@Override
				public void addToStruct(Struct struct, OraCdcStatementBase stmt, OraCdcTransaction transaction) {
					if (addRowScn)
						struct.put(rowScnName, new Struct(WRAPPED_INT64_SCHEMA).put("V", stmt.getScn()));
					if (addRowTs)
						struct.put(rowTsName, new Struct(WRAPPED_UNIXTIME_SCHEMA).put("V", stmt.getTs()));
					if (addCommitScn)
						struct.put(commitScnName, new Struct(WRAPPED_INT64_SCHEMA).put("V", transaction.getCommitScn()));
					if (addRowOp)
						struct.put(rowOpName, new Struct(WRAPPED_STRING_SCHEMA).put("V", stmt.opName()));
					if (addRowXid)
						struct.put(rowXidName, new Struct(WRAPPED_STRING_SCHEMA).put("V", transaction.getXid()));
					if (auditNeeded) {
						if (addUsernameField)
							struct.put(usernameField, new Struct(WRAPPED_OPT_STRING_SCHEMA).put("V", transaction.getUsername()));
						if (addOsUsernameField)
							struct.put(osUsernameField, new Struct(WRAPPED_OPT_STRING_SCHEMA).put("V", transaction.getOsUsername()));
						if (addHostnameField)
							struct.put(osUsernameField, new Struct(WRAPPED_OPT_STRING_SCHEMA).put("V", transaction.getHostname()));
						if (addAuditSessionIdField)
							struct.put(auditSessionIdField, new Struct(WRAPPED_OPT_INT64_SCHEMA).put("V", transaction.getAuditSessionId()));
						if (addSessionInfoField)
							struct.put(sessionInfoField, new Struct(WRAPPED_OPT_STRING_SCHEMA).put("V", transaction.getSessionInfo()));
						if (addClientIdField)
							struct.put(clientIdField, new Struct(WRAPPED_OPT_STRING_SCHEMA).put("V", transaction.getClientId()));
					}
				}
			};
		} else
			handler = new PseudoColumnsHandler() {
				@Override
				public void addToSchema(SchemaBuilder builder) {}
				@Override
				public void addToStruct(Struct struct, OraCdcStatementBase stmt, OraCdcTransaction transaction) {}
			};
	}

	public void addToSchema(final SchemaBuilder builder) {
		handler.addToSchema(builder);
	}

	public void addToStruct(
			final Struct struct, final OraCdcStatementBase stmt, final OraCdcTransaction transaction) {
		handler.addToStruct(struct, stmt, transaction);
	}

	@Override
	public boolean auditNeeded() {
		return auditNeeded;
	}

	@Override
	public boolean userName() {
		return addUsernameField;
	}

	@Override
	public boolean osUserName() {
		return addOsUsernameField;
	}

	@Override
	public boolean hostName() {
		return addHostnameField;
	}

	@Override
	public boolean auditSessionId() {
		return addAuditSessionIdField;
	}

	@Override
	public boolean sessionInfo() {
		return addSessionInfoField;
	}

	@Override
	public boolean clientId() {
		return addClientIdField;
	}

}
