package solutions.a2.cdc.oracle;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcTransaction {


	public void addStatement(final OraCdcLogMinerStatement oraSql);
	//TODO ???
//	public void addStatement(final OraCdcLogMinerStatement oraSql, final List<OraCdcLargeObjectHolder> lobs);
	public boolean getStatement(OraCdcLogMinerStatement oraSql);
	//TODO ???
//	public boolean getStatement(OraCdcLogMinerStatement oraSql, List<OraCdcLargeObjectHolder> lobs);
	//TODO ???
//	public boolean getLobs(final int lobCount, final List<OraCdcLargeObjectHolder> lobs);
	public void close();
	public int length();
	public int offset();	
	//TODO ???
//	public Map<String, Object> attrsAsMap();
	public String getXid();
	public long getFirstChange();
	public long getNextChange();
	public Long getCommitScn();
	public void setCommitScn(Long commitScn);
	//TODO ???
//	public Path getPath();
	public long size();


}
