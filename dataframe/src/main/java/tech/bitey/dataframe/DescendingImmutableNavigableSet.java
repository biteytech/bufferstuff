/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.bitey.dataframe;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;

public class DescendingImmutableNavigableSet<E> extends AbstractSet<E> implements NavigableSet<E> {

	private final NavigableSet<E> asc;
	private final Comparator<? super E> comparator;
	
	public DescendingImmutableNavigableSet(NavigableSet<E> asc) {
		this.asc = asc;
		comparator = (a, b) -> asc.comparator().compare(b, a);
	}
	
	@Override
	public Comparator<? super E> comparator() {
		return comparator;
	}

	@Override
	public E first() {
		return asc.last();
	}

	@Override
	public E last() {
		return asc.first();
	}

	@Override
	public int size() {
		return asc.size();
	}

	@Override
	public boolean contains(Object o) {
		return asc.contains(o);
	}

	@Override
	public boolean add(E e) {
		throw new UnsupportedOperationException("add");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("remove");
	}
	
	@Override
	public E lower(E e) {
		return higher(e);
	}

	@Override
	public E floor(E e) {
		return ceiling(e);
	}

	@Override
	public E ceiling(E e) {
		return floor(e);
	}

	@Override
	public E higher(E e) {
		return lower(e);
	}

	@Override
	public E pollFirst() {
		throw new UnsupportedOperationException("pollFirst");
	}

	@Override
	public E pollLast() {
		throw new UnsupportedOperationException("pollLast");
	}

	@Override
	public Iterator<E> iterator() {
		return asc.descendingIterator();
	}

	@Override
	public NavigableSet<E> descendingSet() {
		return asc;
	}

	@Override
	public Iterator<E> descendingIterator() {
		return asc.iterator();
	}

	@Override
	public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		return asc.subSet(toElement, toInclusive, fromElement, fromInclusive).descendingSet();
	}

	@Override
	public SortedSet<E> subSet(E fromElement, E toElement) {
		return asc.subSet(toElement, false, fromElement, true).descendingSet();
	}

	@Override
	public NavigableSet<E> headSet(E toElement, boolean inclusive) {
		return asc.tailSet(toElement, inclusive).descendingSet();
	}

	@Override
	public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
		return asc.headSet(fromElement, inclusive).descendingSet();
	}

	@Override
	public SortedSet<E> headSet(E toElement) {
		return asc.tailSet(toElement, false).descendingSet();
	}

	@Override
	public SortedSet<E> tailSet(E fromElement) {
		return asc.headSet(fromElement, true).descendingSet();
	}

}
