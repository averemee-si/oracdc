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

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

/**
 * 
 * @author averemee
 *
 */
public class ColumnJdbcTypeRenderer extends DefaultTableCellRenderer {

	private static final long serialVersionUID = -3360330674626012189L;

	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
		if (column == OraTableColumnsDataModel.JDBC_TYPE_COLUMN) {
			setText(JdbcTypes.getTypeName((Integer)value));
			if (isSelected) {
				setBackground(table.getSelectionBackground());
			} else {
				setBackground(table.getSelectionForeground());
			}
			return this;
		} else {
			return super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
		}
	}

}
