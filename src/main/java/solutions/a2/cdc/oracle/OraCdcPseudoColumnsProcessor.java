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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcPseudoColumnsProcessor {

	private final boolean pseudoColumns;

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

	public OraCdcPseudoColumnsProcessor(final OraCdcSourceConnectorConfig config) {
		rowScnName = config.getOraRowScnField();
		if (rowScnName != null) {
			addRowScn = true;
		} else {
			addRowScn = false;
		}
		commitScnName = config.getOraCommitScnField();
		if (commitScnName != null) {
			addCommitScn = true;
		} else {
			addCommitScn = false;
		}
		rowTsName = config.getOraRowTsField();
		if (rowTsName != null) {
			addRowTs = true;
		} else {
			addRowTs = false;
		}
		rowOpName = config.getOraRowOpField();
		if (rowOpName != null) {
			addRowOp = true;
		} else {
			addRowOp = false;
		}
		rowXidName = config.getOraXidField();
		if (rowXidName != null) {
			addRowXid = true;
		} else {
			addRowXid = false;
		}

		usernameField = config.getOraUsernameField();
		if (usernameField != null) {
			addUsernameField = true;
		} else {
			addUsernameField = false;
		}
		osUsernameField = config.getOraOsUsernameField();
		if (osUsernameField != null) {
			addOsUsernameField = true;
		} else {
			addOsUsernameField = false;
		}
		hostnameField = config.getOraHostnameField();
		if (hostnameField != null) {
			addHostnameField = true;
		} else {
			addHostnameField = false;
		}
		auditSessionIdField = config.getOraAuditSessionIdField();
		if (auditSessionIdField != null) {
			addAuditSessionIdField = true;
		} else {
			addAuditSessionIdField = false;
		}
		sessionInfoField = config.getOraSessionInfoField();
		if (sessionInfoField != null) {
			addSessionInfoField = true;
		} else {
			addSessionInfoField = false;
		}
		clientIdField = config.getOraClientIdField();
		if (clientIdField != null) {
			addClientIdField = true;
		} else {
			addClientIdField = false;
		}

		if (addUsernameField || addOsUsernameField || addHostnameField ||
				addAuditSessionIdField || addSessionInfoField || addClientIdField) {
			auditNeeded = true;
		} else {
			auditNeeded = false;
		}

		if (addRowScn || addCommitScn || addRowTs || addRowOp ||
				auditNeeded) {
			pseudoColumns = true;
		} else {
			pseudoColumns = false;
		}
	}

	public void addToSchema(final SchemaBuilder builder) {
		if (pseudoColumns) {
			if (addRowScn) {
				//TODO
				//TODO
				//TODO Will be changed in 3.0 together with SCN datatype replacement to unsigned long
				//TODO
				//TODO
				builder.field(rowScnName, Schema.INT64_SCHEMA);
			}
			if (addRowTs) {
				builder.field(rowTsName, Timestamp.builder().required().build());
			}
			if (addCommitScn) {
				//TODO
				//TODO
				//TODO Will be changed in 3.0 together with SCN datatype replacement to unsigned long
				//TODO
				//TODO
				builder.field(commitScnName, Schema.INT64_SCHEMA);
			}
			if (addRowOp) {
				builder.field(rowOpName, Schema.STRING_SCHEMA);
			}

			if (addRowXid) {
				builder.field(rowXidName, Schema.STRING_SCHEMA);
			}

			if (auditNeeded) {
				if (addUsernameField) {
					builder.field(usernameField, Schema.OPTIONAL_STRING_SCHEMA);
				}
				if (addOsUsernameField) {
					builder.field(osUsernameField, Schema.OPTIONAL_STRING_SCHEMA);
				}
				if (addHostnameField) {
					builder.field(hostnameField, Schema.OPTIONAL_STRING_SCHEMA);
				}
				if (addAuditSessionIdField) {
					builder.field(auditSessionIdField, Schema.OPTIONAL_INT64_SCHEMA);
				}
				if (addSessionInfoField) {
					builder.field(sessionInfoField, Schema.OPTIONAL_STRING_SCHEMA);
				}
				if (addClientIdField) {
					builder.field(clientIdField, Schema.OPTIONAL_STRING_SCHEMA);
				}
			}
		}
	}

	public void addToStruct(
			final Struct struct, final OraCdcStatementBase stmt, final OraCdcTransaction transaction) {
		if (addRowScn) {
			//TODO
			//TODO
			//TODO Will be changed in 3.0 together with SCN datatype replacement to unsigned long
			//TODO
			//TODO
			struct.put(rowScnName, stmt.getScn());
		}
		if (addRowTs) {
			struct.put(rowTsName, stmt.getTimestamp());
		}
		if (addCommitScn) {
			//TODO
			//TODO
			//TODO Will be changed in 3.0 together with SCN datatype replacement to unsigned long
			//TODO
			//TODO
			struct.put(commitScnName, transaction.getCommitScn());
		}
		if (addRowOp) {
			final String operation;
			switch (stmt.getOperation()) {
			case OraCdcV$LogmnrContents.INSERT:
				operation = "INSERT";
				break;
			case OraCdcV$LogmnrContents.UPDATE:
				operation = "UPDATE";
				break;
			case OraCdcV$LogmnrContents.DELETE:
				operation = "DELETE";
				break;
			default:
				// Very rare case...
				operation = "XML DOC BEGIN";
			} 
			struct.put(rowOpName, operation);
		}
		if (addRowXid) {
			struct.put(rowXidName, transaction.getXid());
		}
		if (auditNeeded) {
			if (addUsernameField) {
				struct.put(usernameField, transaction.getUsername());
			}
			if (addOsUsernameField) {
				struct.put(osUsernameField, transaction.getOsUsername());
			}
			if (addHostnameField) {
				struct.put(hostnameField, transaction.getHostname());
			}
			if (addAuditSessionIdField) {
				struct.put(auditSessionIdField, transaction.getAuditSessionId());
			}
			if (addSessionInfoField) {
				struct.put(sessionInfoField, transaction.getSessionInfo());
			}
			if (addClientIdField) {
				struct.put(clientIdField, transaction.getClientId());
			}
		}
	}

	public boolean isAuditNeeded() {
		return auditNeeded;
	}

	public boolean isUsername() {
		return addUsernameField;
	}

	public boolean isOsUsername() {
		return addOsUsernameField;
	}

	public boolean isHostname() {
		return addHostnameField;
	}

	public boolean isAuditSessionId() {
		return addAuditSessionIdField;
	}

	public boolean isSessionInfo() {
		return addSessionInfoField;
	}

	public boolean isClientId() {
		return addClientIdField;
	}

}
