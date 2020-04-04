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

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.ComboBoxModel;
import javax.swing.event.ListDataListener;

/**
 * 
 * @author averemee
 *
 */
public class TablesDataModel implements ComboBoxModel<String> {

	private final List<AbstractMap.SimpleImmutableEntry<Long, String>> indexOfTables;
	private final Set<ListDataListener> listeners;
	private String selectedOraTableName;

	public TablesDataModel(final List<AbstractMap.SimpleImmutableEntry<Long, String>> indexOfTables) {
		this.indexOfTables = indexOfTables;
		listeners = new HashSet<>();
	}

	@Override
	public int getSize() {
		return indexOfTables.size();
	}

	@Override
	public String getElementAt(int index) {
		return indexOfTables.get(index).getValue();
	}

	@Override
	public void addListDataListener(ListDataListener l) {
		listeners.add(l);
	}

	@Override
	public void removeListDataListener(ListDataListener l) {
		listeners.remove(l);
	}

	@Override
	public void setSelectedItem(Object anItem) {
		selectedOraTableName = (String) anItem;
	}

	@Override
	public Object getSelectedItem() {
		return selectedOraTableName;
	}

}
