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

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class ParamConstants {

	public static final String CONNECTION_TNS_ADMIN_PARAM = "a2.tns.admin";
	public static final String CONNECTION_TNS_ADMIN_DOC = "Location of tnsnames.ora file. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String CONNECTION_TNS_ALIAS_PARAM = "a2.tns.alias";
	public static final String CONNECTION_TNS_ALIAS_DOC = "Connection TNS alias. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String STANDBY_WALLET_PARAM = "a2.standby.wallet.location";
	public static final String STANDBY_WALLET_DOC = "Location of Oracle Wallet for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_URL_PARAM = "a2.standby.jdbc.url";
	public static final String STANDBY_URL_DOC = "JDBC connection URL for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_TNS_ADMIN_PARAM = "a2.standby.tns.admin";
	public static final String STANDBY_TNS_ADMIN_DOC = "Location of tnsnames.ora file for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_TNS_ALIAS_PARAM = "a2.standby.tns.alias";
	public static final String STANDBY_TNS_ALIAS_DOC = "Connection TNS alias for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String DICTIONARY_FILE_PARAM = "a2.dictionary.file";
	public static final String DICTIONARY_FILE_DOC = "File with stored columns data type mapping. For more details contact us at oracle@a2-solutions.eu";

	public static final String INITIAL_LOAD_PARAM = "a2.initial.load";
	public static final String INITIAL_LOAD_DOC = "A mode for performing initial load of data from tables when set to EXECUTE. Default - IGNORE";
	public static final String INITIAL_LOAD_IGNORE = "IGNORE";
	public static final String INITIAL_LOAD_EXECUTE = "EXECUTE";
	public static final String INITIAL_LOAD_COMPLETED = "COMPLETED";

	public static final String ARCHIVED_LOG_CAT_PARAM = "a2.archived.log.catalog";
	public static final String ARCHIVED_LOG_CAT_DOC = "name of class which implements solutions.a2.cdc.oracle.OraLogMiner interface. Default - solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl which reads archived log information from V$ARCHIVED_LOG fixed view";
	public static final String ARCHIVED_LOG_CAT_DEFAULT = "solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl";

	public static final String FETCH_SIZE_PARAM = "a2.fetch.size";
	public static final String FETCH_SIZE_DOC = "number of rows fetched with each RDBMS round trip for access V$LOGMNR_CONTENTS. Default 32";
	public static final int FETCH_SIZE_DEFAULT = 32;

	public static final String TRACE_LOGMINER_PARAM = "a2.logminer.trace";
	public static final String TRACE_LOGMINER_DOC = "trace with 'event 10046 level 8' LogMiner calls? Default - false";

	public static final String MAKE_DISTRIBUTED_ACTIVE_PARAM = "a2.distributed.activate";
	public static final String MAKE_DISTRIBUTED_ACTIVE_DOC = "Use oracdc in distributed configuration (redo logs are generated at source RDBMS server and then transferred to compatible target RDBMS server for processing with LogMiner. Default - false"; 

	public static final String DISTRIBUTED_WALLET_PARAM = "a2.distributed.wallet.location";
	public static final String DISTRIBUTED_WALLET_DOC = "Location of Oracle Wallet for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_URL_PARAM = "a2.distributed.jdbc.url";
	public static final String DISTRIBUTED_URL_DOC = "JDBC connection URL for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_TNS_ADMIN_PARAM = "a2.distributed.tns.admin";
	public static final String DISTRIBUTED_TNS_ADMIN_DOC = "Location of tnsnames.ora file for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_TNS_ALIAS_PARAM = "a2.distributed.tns.alias";
	public static final String DISTRIBUTED_TNS_ALIAS_DOC = "Connection TNS alias for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_TARGET_HOST = "a2.distributed.target.host";
	public static final String DISTRIBUTED_TARGET_HOST_DOC = "hostname of the target (where dbms_logmnr runs) database on which the shipment agent is running";

	public static final String DISTRIBUTED_TARGET_PORT = "a2.distributed.target.port";
	public static final String DISTRIBUTED_TARGET_PORT_DOC = "port number on which shipping agent listens for requests";
	public static final int DISTRIBUTED_TARGET_PORT_DEFAULT = 21521;

	public static final String ORA_TRANSACTION_IMPL_PARAM = "a2.transaction.implementation";
	public static final String ORA_TRANSACTION_IMPL_DOC = 
			"Queue implementation for processing SQL statements within transactions.\n" +
			"Allowed values: ChronicleQueue and ArrayList.\n" + 
			"Default - ChronicleQueue.\n" + 
			"LOB processing is only possible if a2.transaction.implementation is set to ChronicleQueue.\n";
	public static final String ORA_TRANSACTION_IMPL_CHRONICLE = "ChronicleQueue";
	public static final String ORA_TRANSACTION_IMPL_JVM = "ArrayList";
	
	public static final String PROCESS_ONLINE_REDO_LOGS_PARAM = "a2.process.online.redo.logs";
	public static final String PROCESS_ONLINE_REDO_LOGS_DOC = 
			"Default - false.\n" +
			"When set to true oracdc process online redo logs.";
	
	public static final String CURRENT_SCN_QUERY_INTERVAL_PARAM = "a2.scn.query.interval.ms";
	public static final String CURRENT_SCN_QUERY_INTERVAL_DOC = 
			"Default - 60_000.\n" +
			"Minimum time in milliseconds to determine the current SCN during online redo log processing.";
	public static final int CURRENT_SCN_QUERY_INTERVAL_DEFAULT = 60_000;

	public static final String PRINT_ALL_ONLINE_REDO_RANGES_PARAM = "a2.print.all.online.scn.ranges";
	public static final String PRINT_ALL_ONLINE_REDO_RANGES_DOC =
			"Default - true.\n" +
			"If set to true oracdc prints detailed information about SCN ranges when working with the online log every time interval specified by the a2.scn.query.interval.ms parameter.\n" +
			"If set to false oracdc prints information about current online redo only when SEQUENCE# is changed.";

}
