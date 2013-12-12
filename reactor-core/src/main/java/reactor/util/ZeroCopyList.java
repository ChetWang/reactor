package reactor.util;

import reactor.util.Assert;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
public class ZeroCopyList<T> implements List<T> {

	private final List<T> head;
	private final int     len;
	private final T       tail;

	public ZeroCopyList(T tail) {
		this(null, tail);
	}

	public ZeroCopyList(List<T> head, T tail) {
		Assert.notNull(tail, "Tail cannot be null.");
		this.head = (null == head ? Collections.<T>emptyList() : head);
		this.len = this.head.size();
		this.tail = tail;
	}

	@Override
	public int size() {
		return len + 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public boolean contains(Object o) {
		return tail.equals(o) || head.contains(o);
	}

	@Override
	public Iterator<T> iterator() {
		return new ZeroCopyIterator();
	}

	@Override
	public Object[] toArray() {
		return toArray(new Object[size()]);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T1> T1[] toArray(T1[] a) {
		System.arraycopy(head.toArray(), 0, a, 0, a.length - 1);
		a[a.length - 1] = (T1)tail;
		return a;
	}

	@Override
	public boolean add(T t) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public boolean remove(Object o) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return head.containsAll(c) || (c.size() == 1 && tail.equals(c.iterator().next()));
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public void clear() {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public T get(int index) {
		return index == len ? tail : head.get(index);
	}

	@Override
	public T set(int index, T element) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public void add(int index, T element) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public T remove(int index) {
		throw new IllegalStateException("zero-copy Lists are immutable");
	}

	@Override
	public int indexOf(Object o) {
		int idx = head.indexOf(o);
		return (idx > -1 ? idx : tail.equals(o) ? len : -1);
	}

	@Override
	public int lastIndexOf(Object o) {
		int idx = head.lastIndexOf(o);
		return (idx > -1 ? idx : tail.equals(o) ? len : -1);
	}

	@Override
	public ListIterator<T> listIterator() {
		throw new IllegalStateException("not implemented");
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		throw new IllegalStateException("not implemented");
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		throw new IllegalStateException("not implemented");
	}

	private class ZeroCopyIterator implements Iterator<T> {
		private final AtomicInteger current = new AtomicInteger(0);

		@Override
		public boolean hasNext() {
			return current.get() <= len;
		}

		@Override
		public T next() {
			int idx = current.getAndIncrement();
			if(idx == len) {
				return tail;
			} else {
				return head.get(idx);
			}
		}

		@Override
		public void remove() {
		}
	}

}
