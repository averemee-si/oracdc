package eu.solutions.a2.cdc.oracle.kafka.connect;

public class ConnectorConfigConstants {

	public static final String CONNECTION_URL_PARAM = "a2.jdbc.url";
	protected static final String CONNECTION_URL_DOC = "JDBC connection URL";

	public static final String CONNECTION_USER_PARAM = "a2.jdbc.username";
	protected static final String CONNECTION_USER_DOC = "JDBC connection user";

	public static final String CONNECTION_PASSWORD_PARAM = "a2.jdbc.password";
	protected static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

	public static final String BATCH_SIZE_PARAM = "a2.batch.size";
	protected static final String BATCH_SIZE_DOC = "Maximum number of statements to include in a single batch when inserting/updating/deleting data";
	public static final int BATCH_SIZE_DEFAULT = 1000;

	public static final String SCHEMA_TYPE_PARAM = "a2.schema.type";
	protected static final String SCHEMA_TYPE_DOC = "Type of schema used by oracdc: Kafka Connect (default) or standalone";
	public static final String SCHEMA_TYPE_KAFKA = "kafka";
	public static final String SCHEMA_TYPE_STANDALONE = "standalone";

}
