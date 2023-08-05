package solutions.a2.cdc.oracle;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcTransaction {


	public void addStatement(final OraCdcLogMinerStatement oraSql);
	public boolean getStatement(OraCdcLogMinerStatement oraSql);
	public void close();
	public int length();
	public int offset();	
	public String getXid();
	public long getFirstChange();
	public long getNextChange();
	public Long getCommitScn();
	public void setCommitScn(Long commitScn);
	public long size();

}
