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

package eu.solutions.a2.cdc.oracle.utils;

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
