package eu.solutions.a2.cdc.oracle.schema;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.OraConnectionObjects;
import eu.solutions.a2.cdc.oracle.OraDictSqlTexts;
import eu.solutions.a2.cdc.oracle.OraRdbmsInfo;
import eu.solutions.a2.cdc.oracle.OraTable4LogMiner;
import eu.solutions.a2.cdc.oracle.ParamConstants;
import eu.solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

public class DatabaseObjects implements ActionListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseObjects.class);

	private final OraRdbmsInfo rdbmsInfo;

	private JComboBox<String> cbPdbs;
	private JComboBox<String> cbOwners;
	private JComboBox<String> cbTables;
	final JComponent[] components;
	private String tablePdb;
	private String tableOwner;
	private String tableName;
	private final OraConnectionObjects oraConnections;

	public DatabaseObjects(
			final String jdbcUrl, final String username, final String password)
					throws SQLException {
		oraConnections = OraConnectionObjects.get4UserPassword("table-schema-editor", jdbcUrl, username, password);
		final Connection connection = oraConnections.getConnection();
		rdbmsInfo = new OraRdbmsInfo(connection);
		final String protoValue = StringUtils.repeat("A", 31);
		cbOwners = new JComboBox<>();
		cbOwners.setPrototypeDisplayValue(protoValue);
		cbOwners.setName("USERNAME");
		cbOwners.addActionListener(this);
		cbTables = new JComboBox<>();
		cbTables.setPrototypeDisplayValue(protoValue);
		cbTables.setName("TABLE_NAME");
		cbTables.addActionListener(this);
		if (rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed()) {
			if (rdbmsInfo.isCdbRoot()) {
				PreparedStatement statement = connection.prepareStatement(
						"select PDB_NAME from CDB_PDBS where PDB_NAME != 'PDB$SEED'",
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = statement.executeQuery();
				cbPdbs = new JComboBox<>();
				cbPdbs.setPrototypeDisplayValue(protoValue);
				cbPdbs.setName("PDB_NAME");
				cbPdbs.addActionListener(this);
				while (rs.next()) {
					cbPdbs.addItem(rs.getString("PDB_NAME"));
				}
				rs.close();
				rs = null;
				statement.close();
				statement = null;
			} else {
				throw new SQLException("Must connect to CDB$ROOT for CDB database!");
			}
			components = new JComponent[] {
					new JLabel("Choose PDB"),
					cbPdbs,
					new JLabel("Choose Owner"),
					cbOwners,
					new JLabel("Choose Table"),
					cbTables
			};
		} else {
			PreparedStatement statement = connection.prepareStatement("select USERNAME from ALL_USERS",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				cbOwners.addItem(rs.getString("USERNAME"));
			}
			components = new JComponent[] {
					new JLabel("Choose Owner"),
					cbOwners,
					new JLabel("Choose Table"),
					cbTables
			};
		}
	}

	protected JComponent[] getComponents() {
		return components;
	}

	@Override
	public void actionPerformed(ActionEvent event) {
		@SuppressWarnings("unchecked")
		JComboBox<String> comboBox  = (JComboBox<String>) event.getSource();
		switch (comboBox.getName()) {
		case "PDB_NAME":
			tablePdb = (String) comboBox.getSelectedItem();
			tableOwner = null;
			tableName = null;
			cbOwners.removeAllItems();
			cbTables.removeAllItems();
			fillPdbTableOwners();
			break;
		case "USERNAME":
			tableOwner = (String) comboBox.getSelectedItem();
			tableName = null;
			cbTables.removeAllItems();
			fillTableNames();
			break;
		case "TABLE_NAME":
			tableName = (String) comboBox.getSelectedItem();
			break;
		}
	}

	public AbstractMap.SimpleImmutableEntry<Long, OraTable4LogMiner> getTableDef() {
		if (StringUtils.isAllEmpty(tableName)) {
			JOptionPane.showMessageDialog(null, "Please choose table!!!",
					"No table selected!", JOptionPane.ERROR_MESSAGE); 
			return null;
		}
		final boolean isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		try (Connection connection = oraConnections.getConnection()) {
			PreparedStatement statement = connection.prepareStatement(
					isCdb ?
						(OraDictSqlTexts.CHECK_TABLE_CDB + " and P.PDB_NAME = ? and O.OWNER = ? and O.OBJECT_NAME = ?") :
						(OraDictSqlTexts.CHECK_TABLE_NON_CDB + " and O.OWNER = ? and O.OBJECT_NAME = ?"),
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			if (isCdb) {
				statement.setString(1, tablePdb);
				statement.setString(2, tableOwner);
				statement.setString(3, tableName);
			} else {
				statement.setString(1, tableOwner);
				statement.setString(2, tableName);
			}
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				final long dataObjectId = rs.getLong("OBJECT_ID");
				final long combinedDataObjectId;
				final long conId;
				if (isCdb) {
					conId = rs.getInt("CON_ID");
					combinedDataObjectId = (conId << 32) | (dataObjectId & 0xFFFFFFFFL); 
				} else {
					conId = 0;
					combinedDataObjectId = dataObjectId;
				}
				//TODO
				//TODO Do we need to pass LOB implementation class as parameter???
				//TODO
				final boolean processLobs = true;
				OraTable4LogMiner oraTable = new OraTable4LogMiner(
						isCdb ? tablePdb : null,
						isCdb ? (short) conId : -1,
						tableOwner, tableName, "ENABLED".equalsIgnoreCase(rs.getString("DEPENDENCIES")),
						ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD, true,
						processLobs, new OraCdcDefaultLobTransformationsImpl(), isCdb,
						null, null, null,
						ParamConstants.TOPIC_NAME_STYLE_INT_TABLE, ParamConstants.TOPIC_NAME_DELIMITER_UNDERSCORE,
						rdbmsInfo, connection);
				return new AbstractMap.SimpleImmutableEntry<Long, OraTable4LogMiner>(combinedDataObjectId, oraTable);
			} else {
				throw new SQLException(
						"Unknown corruption while fetching OBJECT_ID!\nPlease contact Oracle DBA!");
			}
		} catch (SQLException sqle) {
			JOptionPane.showMessageDialog(null, "Database Error!\n" + sqle.getMessage(),
					"Database Error!", JOptionPane.ERROR_MESSAGE); 
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
		return null;
	}

	private void fillPdbTableOwners() {
		try (Connection connection = oraConnections.getConnection()) {
			PreparedStatement statement = connection.prepareStatement(
					"select USERNAME from CDB_USERS U, CDB_PDBS D where U.CON_ID = D.CON_ID and D.PDB_NAME = ?",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, tablePdb);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				cbOwners.addItem(rs.getString("USERNAME"));
			}
			rs.close();
			rs = null;
			statement.close();
			statement = null;
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

	private void fillTableNames() {
		try (Connection connection = oraConnections.getConnection()) {
			PreparedStatement statement = connection.prepareStatement(
					rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed() ?
						"select TABLE_NAME from CDB_TABLES T, CDB_USERS U, CDB_PDBS D where U.CON_ID = D.CON_ID and T.OWNER = U.USERNAME and D.PDB_NAME = ? and U.USERNAME = ?" :
						"select TABLE_NAME from ALL_TABLES T, ALL_USERS U where T.OWNER = U.USERNAME and U.USERNAME = ?",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			if (rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed()) {
				statement.setString(1, tablePdb);
				statement.setString(2, tableOwner);
			} else {
				statement.setString(1, tableOwner);
			}
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				cbTables.addItem(rs.getString("TABLE_NAME"));
			}
			rs.close();
			rs = null;
			statement.close();
			statement = null;
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

}
