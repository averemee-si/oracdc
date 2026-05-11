/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.utils;

import java.util.ArrayList;

/**
 * Based on https://stackoverflow.com/questions/1963806/is-there-a-fixed-sized-queue-which-removes-excessive-elements
 * 
 *
 * @param <T>
 */
public class LimitedSizeQueue<T> extends ArrayList<T> {

	private static final long serialVersionUID = 5399403217937862057L;
	private int maxSize;

	public LimitedSizeQueue(int size) {
		super();
		this.maxSize = size;
	}

	@Override
	public boolean add(T listMember) {
		boolean result = false;
		synchronized (this) {
			result = super.add(listMember);
		}
		if (this.size() > maxSize) {
			synchronized (this) {
				this.removeRange(0, size() - maxSize);
			}
		}
		return result;
	}

	public T getOldest() {
		if (this.size() > 0) {
			return this.get(0);
		} else {
			return null;
		}
	}

	public T getYoungest() {
		if (this.size() > 0) {
			return this.get(this.size() - 1);
		} else {
			return null;
		}
	}

}
