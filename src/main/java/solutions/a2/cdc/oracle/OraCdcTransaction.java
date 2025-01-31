package solutions.a2.cdc.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcTransaction {


	public void addStatement(final OraCdcStatementBase oraSql);
	public boolean getStatement(OraCdcStatementBase oraSql);
	public void close();
	public int length();
	public int offset();	
	public String getXid();
	public long getFirstChange();
	public long getNextChange();
	public long getCommitScn();
	public void setCommitScn(long commitScn);
	public void setCommitScn(long commitScn, OraCdcPseudoColumnsProcessor pseudoColumns, ResultSet resultSet) throws SQLException;
	public long size();

	public String getUsername();
	public String getOsUsername();
	public String getHostname();
	public long getAuditSessionId();
	public String getSessionInfo();
	public String getClientId();
	public boolean startsWithBeginTrans();
	
}
