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
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.utils;

import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

public class LimitedSizeQueue<T> extends AbstractCollection<T> {

	private int maxSize;
	private final ArrayDeque<T> elements;

	public LimitedSizeQueue(final int size) {
		if (size < 1)
			throw new IllegalArgumentException("The size of LimitedSizeQueue must be greater than zero!");
		this.maxSize = size;
		this.elements = new ArrayDeque<>(size);
	}

	@Override
	public boolean add(final T element) {
		Objects.requireNonNull(element, "LimitedSizeQueue does not permit null elements!");
		synchronized (this) {
			elements.addLast(element);
			while (elements.size() > maxSize)
				elements.pollFirst();
		}
		return true;
	}

	public T getOldest() {
		synchronized (this) {
			return elements.peekFirst();
		}
	}

	public T getYoungest() {
		synchronized (this) {
			return elements.peekLast();
		}
	}

	public T pollOldest() {
		synchronized (this) {
			return elements.pollFirst();
		}
	}

	public int maxSize() {
		return maxSize;
	}

	@Override
	public int size() {
		synchronized (this) {
			return elements.size();
		}
	}

	@Override
	public boolean isEmpty() {
		synchronized (this) {
			return elements.isEmpty();
		}
	}

	@Override
	public boolean contains(final Object o) {
		synchronized (this) {
			return elements.contains(o);
		}
	}

	@Override
	public void clear() {
		synchronized (this) {
			elements.clear();
		}
	}

	@Override
	public Iterator<T> iterator() {
		final Collection<T> snapshot;
		synchronized (this) {
			snapshot = new ArrayList<>(elements);
		}
		return snapshot.iterator();
	}

	@Override
	public Object[] toArray() {
		synchronized (this) {
			return elements.toArray();
		}
	}

	@Override
	public <E> E[] toArray(final E[] a) {
		synchronized (this) {
			return elements.toArray(a);
		}
	}

	@Override
	public String toString() {
		synchronized (this) {
			return elements.toString();
		}
	}
}
