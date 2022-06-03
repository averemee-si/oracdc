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

import java.awt.Component;

import javax.swing.AbstractCellEditor;
import javax.swing.JComboBox;
import javax.swing.JTable;
import javax.swing.table.TableCellEditor;

/**
 * 
 * @author averemee
 *
 */
public class ColumnJdbcTypeEditor extends AbstractCellEditor implements TableCellEditor {

	private static final long serialVersionUID = 3247501720801922177L;

	private Integer jdbcType;

	@Override
	public Object getCellEditorValue() {
		return jdbcType;
	}

	@Override
	public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
		if (column == OraTableColumnsDataModel.JDBC_TYPE_COLUMN) {
			JComboBox<String> comboBox = new JComboBox<String>();
			for (String type : JdbcTypes.NUMERICS) {
				comboBox.addItem(type);
			}

			jdbcType = (Integer)value;
			comboBox.setSelectedItem(JdbcTypes.getTypeName(jdbcType));
			comboBox.addActionListener(event -> {
				@SuppressWarnings("unchecked")
				JComboBox<String> cb = (JComboBox<String>) event.getSource();
				jdbcType = JdbcTypes.getTypeId((String) cb.getSelectedItem());
			});

			if (isSelected) {
				comboBox.setBackground(table.getSelectionBackground());
			} else {
				comboBox.setBackground(table.getSelectionForeground());
			}
			return comboBox;
		} else {
			return null;
		}
	}

}
