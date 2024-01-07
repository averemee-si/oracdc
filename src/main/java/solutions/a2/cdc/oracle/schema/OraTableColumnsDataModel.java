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

package solutions.a2.cdc.oracle.schema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

import solutions.a2.cdc.oracle.OraColumn;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraTableColumnsDataModel implements TableModel {

	public static final int JDBC_TYPE_COLUMN = 1;

	private final List<OraColumn> columns;
	private final Set<TableModelListener> listeners;
	private final TableSchemaEditor tse;
	
	public OraTableColumnsDataModel(final List<OraColumn> columns, final TableSchemaEditor tse) {
		this.columns = columns;
		this.listeners = new HashSet<TableModelListener>();
		this.tse = tse;
	}

	@Override
	public int getRowCount() {
		return columns.size();
	}

	@Override
	public int getColumnCount() {
		return 4;
	}

	@Override
	public String getColumnName(int columnIndex) {
		switch (columnIndex) {
		case 0:
			return "Column Name";
		case 1:
			return "Column Type";
		case 2:
			return "Part of PK?";
		case 3:
			return "Nulls allowed?";
		}
		return null;
	}

	@Override
	public Class<?> getColumnClass(int columnIndex) {
		switch (columnIndex) {
		case 0:
			return String.class;
		case 1:
			return Integer.class;
		case 2:
		case 3:
			return Boolean.class;
		default:
			return String.class;
		}
	}

	@Override
	public boolean isCellEditable(int rowIndex, int columnIndex) {
		// Only type editable!!!
		if (columnIndex == JDBC_TYPE_COLUMN) {
			// Bad story only with Oracle NUMBER - human intervention required
			return JdbcTypes.isNumeric(columns.get(rowIndex).getJdbcType());
		} else {
			return false;
		}
	}

	@Override
	public Object getValueAt(int rowIndex, int columnIndex) {
		final OraColumn row = columns.get(rowIndex);
		switch (columnIndex) {
		case 0:
			return row.getColumnName();
		case 1:
			return row.getJdbcType();
		case 2:
			return row.isPartOfPk();
		case 3:
			return row.isNullable();
		}
		return null;
	}

	@Override
	public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
		if (columnIndex == JDBC_TYPE_COLUMN) {
			tse.enableSave();
			columns.get(rowIndex).setJdbcType((Integer) aValue);
		}
	}

	@Override
	public void addTableModelListener(TableModelListener l) {
		listeners.add(l);		
	}

	@Override
	public void removeTableModelListener(TableModelListener l) {
		listeners.remove(l);
	}

	public void removeRow(final JTable tableSchema) {
		final int rowIndex = tableSchema.getSelectedRow();
		boolean removeRow = false;
		if (rowIndex < 0) {
			JOptionPane.showMessageDialog(null,
					"No table column selected for removal!",
					"No column to remove", JOptionPane.ERROR_MESSAGE); 
		} else {
			final OraColumn row = columns.get(rowIndex);
			if (row.isPartOfPk()) {
				JOptionPane.showMessageDialog(null,
						"Unable to delete primary key definition!",
						"Unable to remove PK", JOptionPane.ERROR_MESSAGE); 
			} else if (row.isNullable()) {
				removeRow = true;
			} else {
				final int response = JOptionPane.showConfirmDialog(null,
						"Do you want to remove non null column?",
						"Confirm", JOptionPane.YES_NO_OPTION,
						JOptionPane.QUESTION_MESSAGE);
				if (response == JOptionPane.YES_OPTION) {
					removeRow = true;
				}
			}
		}
		if (removeRow) {
			columns.remove(tableSchema.convertRowIndexToModel(rowIndex));
			tableSchema.revalidate();
			tse.enableSave();
		}
	}

}
