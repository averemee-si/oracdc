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

package eu.solutions.a2.cdc.oracle.schema;

import java.awt.BorderLayout;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.OraColumn;
import eu.solutions.a2.cdc.oracle.OraTable4LogMiner;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * @author averemee
 *
 */
public class TableSchemaEditor extends JFrame {

	private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaEditor.class);
	private static final long serialVersionUID = 2286500183403204546L;

	private Map<Long, OraTable4LogMiner> tables = new ConcurrentHashMap<>();
	private String dictFileName;
	private boolean dataChanged;
	private DatabaseObjects dbObjects;
	private final JButton btnRemoveRow;
	private final JButton btnRemoveTable;
	private final JButton btnAddTable;
	private boolean connectedToDb = false;
	private boolean fileInited = false;

	private final JFileChooser fileChooser;
	private final List<AbstractMap.SimpleImmutableEntry<Long, String>> indexOfTables;
	private final JTable tableSchema = new JTable();
	private final JComboBox<String> comboBoxTables;
	private final EditorProperties props;

	private final JMenuItem menuItemFileOpen = new JMenuItem("Open...", KeyEvent.VK_O);
	private final JMenuItem menuItemFileNew = new JMenuItem("New", KeyEvent.VK_N);
	private final JMenuItem menuItemFileSave = new JMenuItem("Save", KeyEvent.VK_S);
	private final JMenuItem menumenuItemFileSaveAs = new JMenuItem("Save As...", KeyEvent.VK_A);
	private final JMenuItem menuItemQuit = new JMenuItem("Quit", KeyEvent.VK_Q);
	private final JMenuItem menuItemConnect = new JMenuItem("Connect...", KeyEvent.VK_C);
	private final JMenuItem menuItemDisconnect = new JMenuItem("Disconnect", KeyEvent.VK_I);


	public TableSchemaEditor() {
		super("oracdc Source Tables Schema Editor");
		setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		setDefaultLookAndFeelDecorated(true);
		// Prepare list of tables
		indexOfTables = new ArrayList<>();
		tables = new ConcurrentHashMap<>();
		fileInited = true;
		comboBoxTables = new JComboBox<>(new TablesDataModel(indexOfTables));
		dataChanged = false;
		props = new EditorProperties();

		JPanel bottomPanel = new JPanel();
		bottomPanel.setLayout(new BorderLayout());
		btnRemoveRow = new JButton("Remove selected column");
		btnRemoveRow.setEnabled(false);
		btnRemoveRow.addActionListener(event -> {
			final OraTableColumnsDataModel dm = (OraTableColumnsDataModel) tableSchema.getModel();
			dm.removeRow(tableSchema);
		});
		bottomPanel.add(btnRemoveRow, BorderLayout.EAST);

		JPanel topPanel = new JPanel();
		topPanel.setLayout(new BorderLayout());
		btnRemoveTable = new JButton("Remove current table...");
		btnRemoveTable.setEnabled(false);
		btnRemoveTable.addActionListener(event -> {
			if (indexOfTables.size() > 0) {
				final int index = comboBoxTables.getSelectedIndex();
				final long combinedDataObjectId = indexOfTables.get(index).getKey();
				int response = JOptionPane.showConfirmDialog(null,
						"Do you want to remove " + tables.get(combinedDataObjectId).fqn() + " definition?",
						"Confirm", JOptionPane.YES_NO_OPTION,
						JOptionPane.QUESTION_MESSAGE);
				if (response == JOptionPane.YES_OPTION) {
					LOGGER.info("Removing {} definition.", tables.get(combinedDataObjectId).fqn());
					tables.remove(combinedDataObjectId);
					indexOfTables.remove(index);
					comboBoxTables.setModel(new TablesDataModel(indexOfTables));
					comboBoxTables.setSelectedIndex(0);
					enableSave();
				}
			} else {
				JOptionPane.showMessageDialog(null, "Nothing to remove!!!",
						"Removal error", JOptionPane.ERROR_MESSAGE); 
			}
		});
		btnAddTable = new JButton("Load new table definition from DB...");
		btnAddTable.setEnabled(false);
		btnAddTable.addActionListener(event -> {
			final int result = JOptionPane.showConfirmDialog(null, dbObjects.components, "Choose table",
					JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);
			if (result == JOptionPane.OK_OPTION) {
				AbstractMap.SimpleImmutableEntry<Long, OraTable4LogMiner> newTableDef = dbObjects.getTableDef();
				if (newTableDef != null) {
					LOGGER.info("Adding {} to editor.", newTableDef.getValue().fqn());
					tables.put(newTableDef.getKey(), newTableDef.getValue());
					indexOfTables.add(
							new AbstractMap.SimpleImmutableEntry<Long, String>(
									newTableDef.getKey(), newTableDef.getValue().fqn()));
					if (!comboBoxTables.isEnabled()) {
						comboBoxTables.setEnabled(true);
					}
					int selected = comboBoxTables.getSelectedIndex();
					comboBoxTables.setModel(new TablesDataModel(indexOfTables));
					if (selected > -1) {
						comboBoxTables.setSelectedIndex(selected);
					} else {
						try {
							comboBoxTables.setSelectedIndex(0);
						} catch (Exception e) {}
					}
					enableSave();
				}
			}
		});
		topPanel.add(btnRemoveTable, BorderLayout.EAST);
		topPanel.add(btnAddTable, BorderLayout.WEST);

		comboBoxTables.addActionListener(event -> {
			@SuppressWarnings("unchecked")
			JComboBox<String> cb = (JComboBox<String>) event.getSource();
			List<OraColumn> allColumns = 
					tables.get(indexOfTables.get(cb.getSelectedIndex()).getKey()).getAllColumns();
			OraTableColumnsDataModel cdm = new OraTableColumnsDataModel(allColumns, this);
			dataChanged = false;
			tableSchema.setModel(cdm);
			btnRemoveRow.setEnabled(true);
			btnRemoveTable.setEnabled(true);
			tableSchema.getColumnModel().getColumn(0).setPreferredWidth(256);
			tableSchema.getColumnModel().getColumn(1).setPreferredWidth(96);
			tableSchema.getColumnModel().getColumn(2).setPreferredWidth(96);
			tableSchema.getColumnModel().getColumn(3).setPreferredWidth(96);
		});
		comboBoxTables.setPrototypeDisplayValue(StringUtils.repeat("A", 48));
		comboBoxTables.setEnabled(false);

		tableSchema.setDefaultRenderer(Integer.class, new ColumnJdbcTypeRenderer());
		tableSchema.setDefaultEditor(Integer.class, new ColumnJdbcTypeEditor());

		fileChooser = new JFileChooser();
		setupMenu(comboBoxTables);

		JScrollPane scrollPane = new JScrollPane(tableSchema);
		scrollPane.setPreferredSize(new Dimension(576, 512));

		JPanel contents = new JPanel();
		contents.setLayout(new BoxLayout(contents, BoxLayout.Y_AXIS));
		contents.add(comboBoxTables);
		contents.add(topPanel);
		contents.add(scrollPane);
		contents.add(bottomPanel);
		contents.setBorder(BorderFactory.createEmptyBorder(20,20,20,20));
		contents.setOpaque(true);
		setContentPane(contents);
		pack();
		setVisible(true);
	}

	private void setupMenu(final JComboBox<String> comboBoxTables) {
		JMenuBar menuBar = new JMenuBar();

		JMenu menuFile = new JMenu("File");
		menuFile.setMnemonic(KeyEvent.VK_F);
		menuBar.add(menuFile);

		menuItemFileNew.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_N, ActionEvent.ALT_MASK));
		menuItemFileNew.addActionListener(event -> {
			//TODO
			//TODO Ask for Save needed....
			//TODO
			tables = new ConcurrentHashMap<>();
			menuItemFileSave.setEnabled(false);
			fileInited = true;
		});
		menuItemFileNew.setEnabled(true);
		menuFile.add(menuItemFileNew);

		menuFile.addSeparator();

		menuItemFileOpen.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.ALT_MASK));
		menuItemFileOpen.addActionListener(event -> {
			final String lastDir = props.get("dir");
			if (!StringUtils.isEmpty(lastDir)) {
				fileChooser.setCurrentDirectory(new File(lastDir));
			}
			fileChooser.setName(props.get("file"));
			int result = fileChooser.showDialog(null, "Open File...");
			if (result == JFileChooser.APPROVE_OPTION) {
				File file = fileChooser.getSelectedFile();
				try {
					//TODO
					//TODO Ask for Save needed....
					//TODO
					FileLoader loader = new FileLoader(this, file);
					tables = loader.doInBackground();
					indexOfTables.clear();
					for (long combinedDataObjectId : tables.keySet()) {
						OraTable4LogMiner oraTable = tables.get(combinedDataObjectId);
						indexOfTables.add(
								new SimpleImmutableEntry<Long, String>(combinedDataObjectId, oraTable.fqn()));
					}
					comboBoxTables.setEnabled(true);
					comboBoxTables.setModel(new TablesDataModel(indexOfTables));
					if (indexOfTables.size() > 0) {
						comboBoxTables.setSelectedIndex(0);
					}
					dictFileName = file.getAbsolutePath();
					this.setTitle(file.getName());
					props.putFileParams(file.getParent());
					menumenuItemFileSaveAs.setEnabled(true);
					fileInited = true;
					if (connectedToDb) {
						btnAddTable.setEnabled(true);
					}
				} catch (Exception e) {
					JOptionPane.showMessageDialog(null,
							"Wrong JSON file " + file.getAbsolutePath() ,
							"Unable to open/parse file", JOptionPane.ERROR_MESSAGE); 
					LOGGER.error("Wrong JSON file with dictionary!");
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				} finally {
					this.setCursor(Cursor.getDefaultCursor());
				}
			}
		});
		menuFile.add(menuItemFileOpen);

		menuItemFileSave.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, ActionEvent.ALT_MASK));
		menuItemFileSave.setEnabled(false);
		menuItemFileSave.addActionListener(event -> {
			try {
				FileUtils.writeDictionaryFile(tables, dictFileName);
				menuItemFileSave.setEnabled(false);
			} catch (Exception e) {
				JOptionPane.showMessageDialog(null,
						"Unable to save dictionary to " + dictFileName,
						"Save file error", JOptionPane.ERROR_MESSAGE); 
				LOGGER.error("Save error!!!");
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			}
		});
		menuFile.add(menuItemFileSave);

		menumenuItemFileSaveAs.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_A, ActionEvent.ALT_MASK));
		menumenuItemFileSaveAs.setEnabled(true);
		menumenuItemFileSaveAs.addActionListener(event -> {
			fileChooser.setName(dictFileName);
			int result = fileChooser.showSaveDialog(this);
			if (result == JFileChooser.APPROVE_OPTION) {
				final File file = fileChooser.getSelectedFile();
				boolean continueSave = true;
				try {
					if (file.exists()) {
						int response = JOptionPane.showConfirmDialog(null,
								"Do you want to replace the existing file?",
								"Confirm", JOptionPane.YES_NO_OPTION,
								JOptionPane.QUESTION_MESSAGE);
						if (response != JOptionPane.YES_OPTION) {
							continueSave = false;
						}
					}
					if (continueSave) {
						FileUtils.writeDictionaryFile(tables, file.getAbsolutePath());
						dictFileName = file.getAbsolutePath();
						this.setTitle(file.getName());
						props.putFileParams(file.getParent());
						menuItemFileSave.setEnabled(false);
						dataChanged = false;
					}
				} catch (Exception e) {
					JOptionPane.showMessageDialog(null,
							"Unable to save dictionary to " + file.getAbsolutePath(),
							"Save file error", JOptionPane.ERROR_MESSAGE); 
					LOGGER.error("Save error!!!");
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				}
			}
		});
		menuFile.add(menumenuItemFileSaveAs);

		menuFile.addSeparator();

		menuItemQuit.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, ActionEvent.ALT_MASK));
		menuItemQuit.addActionListener(event -> {
			boolean continueExit = true;
			if (dataChanged) {
				int response = JOptionPane.showConfirmDialog(null,
						"File not saved. Do you want to continue?", "Confirm",
						JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE);
				if (response != JOptionPane.YES_OPTION) {
					continueExit = false;
				}
			}
			if (continueExit) {
				this.setVisible(false);
				this.dispose();
			}
		});
		menuFile.add(menuItemQuit);

		JMenu menuDb = new JMenu("Database");
		menuDb.setMnemonic(KeyEvent.VK_D);
		menuBar.add(menuDb);

		menuItemConnect.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_C, ActionEvent.ALT_MASK));
		menuItemConnect.addActionListener(event -> {
			// jdbc:oracle:thin@<HOST>:<PORT>:<DATABASE_NAME>
			JTextField jtfHost = new JTextField(props.get("host"));
			JTextField jtfPort = new JTextField(props.get("port"));
			JTextField jtfSid = new JTextField(props.get("sid"));
			JTextField jtfUsername = new JTextField(props.get("user"));
			JPasswordField jtfPassword = new JPasswordField();
			final JComponent[] inputs = new JComponent[] {
					new JLabel("Oracle Host"),
					jtfHost,
					new JLabel("Oracle Port"),
					jtfPort,
					new JLabel("Oracle SID"),
					jtfSid,
					new JLabel("Username"),
					jtfUsername,
					new JLabel("Password"),
					jtfPassword
			};
			int result = JOptionPane.showConfirmDialog(null, inputs, "Connect to Oracle",
					JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);
			if (result == JOptionPane.OK_OPTION) {
				final String oracleHost = jtfHost.getText();
				final String oraclePort = jtfPort.getText();
				final String oracleSid = jtfSid.getText();
				final String username = jtfUsername.getText();
				final String password = new String(jtfPassword.getPassword());
				if (!StringUtils.isEmpty(oracleHost)
						&& !StringUtils.isEmpty(oraclePort)
						&& !StringUtils.isEmpty(oracleSid)
						&& !StringUtils.isEmpty(username)
						&& !StringUtils.isEmpty(password)) {
					// jdbc:oracle:thin@<HOST>:<PORT>:<SID>
					final String jdbcUrl = "jdbc:oracle:thin:@//" + oracleHost + ":" + oraclePort + "/" + oracleSid;
					try {
						dbObjects = new DatabaseObjects(jdbcUrl, username, password);
						props.putOraParams(oracleHost, oraclePort, oracleSid, username);
						connectedToDb = true;
						if (fileInited) {
							btnAddTable.setEnabled(true);
						}
						menuItemDisconnect.setEnabled(true);
						menuItemConnect.setEnabled(false);
					} catch (SQLException sqle) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
						JOptionPane.showMessageDialog(null,
								"Unable to establish database connection to \"" + jdbcUrl + "\"\n" +
										"Please check error log and/or console output!",
								"Database Connection Error", JOptionPane.ERROR_MESSAGE); 
					}
				} else {
					JOptionPane.showMessageDialog(null,
							"All parameters must be set!",
							"Database Connection Error", JOptionPane.ERROR_MESSAGE); 
				}
			}
		});
		menuDb.add(menuItemConnect);

		menuItemDisconnect.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_I, ActionEvent.ALT_MASK));
		menuItemDisconnect.addActionListener(event -> {
			dbObjects.destroy();
			dbObjects = null;
			connectedToDb = false;
			menuItemDisconnect.setEnabled(false);
			menuItemConnect.setEnabled(true);
			btnAddTable.setEnabled(false);
		});
		menuItemDisconnect.setEnabled(false);
		
		menuDb.add(menuItemDisconnect);

		this.setJMenuBar(menuBar);
	}

	protected void enableSave() {
		if (!StringUtils.isEmpty(dictFileName)) {
			menuItemFileSave.setEnabled(true);
		}
		dataChanged = true;
	}

	private static class FileLoader extends SwingWorker<Map<Long, OraTable4LogMiner>, Void> {
		private final File file;

		FileLoader(final JFrame frame, final File file) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			this.file = file;
		}
		@Override
		protected Map<Long, OraTable4LogMiner> doInBackground() throws Exception {
			return FileUtils.readDictionaryFile(file, null);
		}
	}

	public static void main(String[] argv) throws IOException {
		//TODO
		//TODO - check variables first....
		//TODO
		BasicConfigurator.configure();
		org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

		try {
			for (LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
				if ("Nimbus".equals(info.getName())) {
					UIManager.setLookAndFeel(info.getClassName());
					break;
				}
			}
		} catch (Exception e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
		}

		SwingUtilities.invokeLater(() -> {
			new TableSchemaEditor();
		});
	}

}
