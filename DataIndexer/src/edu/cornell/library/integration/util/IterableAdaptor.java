
package edu.cornell.library.integration.util;

import java.util.Enumeration;
import java.util.Iterator;

/**
 * Adapts Enumerations and Iterators to be Iterable
 * @param <T> type
 */
public class IterableAdaptor<T> implements Iterable<T> {
	/**
	 * enumeration
	 */
	private final Enumeration<T> en;
	/**
	 * iterator
	 */
	private final Iterator<T> it;
	
	/**
	 * sometimes you have an Enumeration and you want an Iterable
	 * @param en enumeration to adapt
	 */
	public IterableAdaptor(Enumeration<T> en) {
		this.en = en;
		this.it = null;
	}
	
	/**
	 * Accessor for en
	 * @return en
	 */
	protected final Enumeration<T> getEn() {
		return this.en;
	}
	
	/**
	 * sometimes you have an Iterator but you want to use a for
	 * @param it iterator to adapt
	 */
	public IterableAdaptor(Iterator<T> it) {
		this.it = it;
		this.en = null;
	}
	
	// return an adaptor for the Enumeration
  public Iterator<T> iterator() {
		if(this.en != null) {
			return new Iterator<T>() {
				 
				public boolean hasNext() {
					return getEn().hasMoreElements();
				}
				
				 
				public T next() {
					return getEn().nextElement();
				}
				
				 
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		} else if(this.it != null) {
			return this.it;
		} else {
			return new Iterator<T>() {
				 
				public boolean hasNext() {
					return false;
				}
				
				 
				public T next() {
					return null;
				}
				
				 
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
		
	}
	
	/**
	 * sometimes you have an Enumeration and you want an Iterable
	 * @param <T> type
	 * @param enin enumeration to adapt
	 * @return an iterable adapter for the enumeration
	 */
	public static <T> Iterable<T> adapt(Enumeration<T> enin) {
		return new IterableAdaptor<T>(enin);
	}
	
	/**
	 * sometimes you have an Iterator but you want to use a for
	 * @param <T> type
	 * @param itin iterator to adapt
	 * @return an iterable adapter for the iterator
	 */
	public static <T> Iterable<T> adapt(Iterator<T> itin) {
		return new IterableAdaptor<T>(itin);
	}
}
