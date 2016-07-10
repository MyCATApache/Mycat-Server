package io.mycat.memory.helper;

import java.util.List;

public interface InplaceList<E> extends List<E> {

  /**
   * Copies the element at index into dest.
   *
   * @param dest  the destination object
   * @param index the index of the object to get
   * @return the fetched object
   */
  E get(E dest, int index);

  /**
   * Swaps two elements.
   *
   * @param i index of first element to swap
   * @param j index of second element to swap
   */
  void swap(int i, int j);
}
