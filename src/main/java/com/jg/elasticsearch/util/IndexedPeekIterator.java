package com.jg.elasticsearch.util;

import java.util.Iterator;
import java.util.NoSuchElementException;


public final class IndexedPeekIterator<E> implements Iterator<E> {

  private final Iterator<? extends E> iterator;
  private E peek;
  private boolean hasPeek = false;
  private int index = -1;
  private E current = null;

  /**
   * Creates a {@link IndexedPeekIterator}.
   * 
   * @param iterator
   *          an Iterator
   */
  public IndexedPeekIterator(Iterator<? extends E> iterator) {
    this.iterator = iterator;
  }

  private void peeking() {
    peek = iterator.next();
    hasPeek = true;
  }

  /**
   * Returns the index of last returned element. If there is no element has been
   * returned, it returns -1.
   * 
   * @return the index of last returned element
   */
  public int getIndex() {
    return index;
  }

  /**
   * Returns the last returned element. If {@link #next()} has never been
   * called, it returns null.
   * 
   * @return the last returned element
   */
  public E getCurrent() {
    return current;
  }

  
  public boolean hasNext() {
    return hasPeek || iterator.hasNext();
  }

  
  public E next() {
    if (!hasNext()) throw new NoSuchElementException();

    index++;
    if (hasPeek) {
      hasPeek = false;
      return current = peek;
    } else {
      peeking();
      return next();
    }
  }

  
  public void remove() {
    if (hasPeek) throw new IllegalStateException();

    iterator.remove();
  }

  /**
   * Peeks an element advanced. Warning: remove() is temporarily out of function
   * after a peek() until a next() is called.
   * 
   * @return element
   */
  public E peek() {
    if (!hasPeek && hasNext()) peeking();
    if (!hasPeek) throw new NoSuchElementException();

    return peek;
  }

}
