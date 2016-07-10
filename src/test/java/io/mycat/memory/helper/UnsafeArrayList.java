package io.mycat.memory.helper;

import sun.misc.Unsafe;

import java.util.AbstractList;
import java.util.Collection;
import java.util.RandomAccess;

/**
 * ArrayList implemented using Unsafe operations
 *
 * <p>This is not thread safe.
 *
 * @see <a href="http://www.docjar.com/docs/api/sun/misc/Unsafe.html">http://www.docjar.com/docs/api/sun/misc/Unsafe.html</a>
 */
public class UnsafeArrayList<T> extends AbstractList<T> implements InplaceList<T>, RandomAccess {

  private static final int DEFAULT_CAPACITY = 10;
  final Class<T> type;
  final long firstFieldOffset; // Offset to the first field
  final long elementSize;      // Size of all the fields in the object
  final long elementSpacing;   // Distance between offsets in the array. Always >= elementSize
  private T tmp;

  final Unsafe unsafe;
  private UnsafeCopier copier;

  private long base = 0;

  private int size = 0;
  private int capacity = 0;

  /**
   * @param type Must match the parametrised type
   */
  public UnsafeArrayList(Class<T> type) {
    this(type, DEFAULT_CAPACITY);
  }

  public UnsafeArrayList(Class<T> type, Collection<? extends T> c) {
    this(type, c.size());
    addAll(c);
  }

  public UnsafeArrayList(Class<T> type, int initialCapacity) {
    this.type = type;
    this.unsafe = UnsafeHelper.getUnsafe();
    this.firstFieldOffset = UnsafeHelper.firstFieldOffset(type);

    this.elementSize = UnsafeHelper.sizeOf(type) - firstFieldOffset;
    this.elementSpacing = Math.max(8, this.elementSize); // TODO(bramp) Do we need to pad to 8 bytes.

    try {
      copier = new UnrolledUnsafeCopierBuilder().of(type).build(this.unsafe);
      tmp = newInstance();
    } catch (Exception e) {
    }

    setCapacity(initialCapacity);
  }

  @SuppressWarnings("unchecked")
  private T newInstance() throws InstantiationException {
    return (T) unsafe.allocateInstance(type);
  }

  private void setCapacity(int capacity) {
    assert (capacity >= 0);
    this.capacity = capacity;
    base = unsafe.reallocateMemory(base,elementSpacing * capacity);
  }

  public void ensureCapacity(int capacity) {
    // If we don't have enough room, grow by 2x
    if (capacity > this.capacity) {
      setCapacity(capacity + (capacity >> 1));
    }
  }

  protected void checkBounds(int index) throws IndexOutOfBoundsException {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException();
    }
  }

  protected void checkBoundsForAdd(int index) throws IndexOutOfBoundsException {
    if (index < 0 || index > size) {
      throw new IndexOutOfBoundsException();
    }
  }

  @Override public T get(int index) {
    try {
      return get(newInstance(), index);
    } catch (InstantiationException e) {

    }
    return null;
  }

  private long offset(int index) {
    return base + index * elementSpacing;
  }

  /**
   * Copies the element at index into dest
   *
   * @param dest  The destination object
   * @param index The index of the object to get
   * @return The fetched object
   */
  public T get(T dest, int index) {
    checkBounds(index);
    // We would rather do
    //   UnsafeHelper.copyMemory(null, offset(index), dest, firstFieldOffset, elementSize);
    // but this is unsupported by the Unsafe class
    copier.copy(dest, offset(index));
    return dest;
  }

  @Override public T set(int index, T element) {
    checkBounds(index);
    Preconditions.checkNotNull(element);

    // TODO checkIsX()
    // TODO If we try and store an subclass of type, we will slice of some fields.
    //      Instead we should throw an exception.
    T obj = get(index);
    setNoCheck(index, element);
    return obj;
  }

  private void setNoCheck(int index, T element) {
    unsafe.copyMemory(element, firstFieldOffset, null, offset(index), elementSize);
  }

    /**
     * Swaps two elements
     *
     * @param i Index of first element
     * @param j Index of second element
     */
  public void swap(int i, int j) {
    checkBounds(i);
    checkBounds(j);

    if (i == j)
      return;

    copier.copy(tmp, offset(i));
    unsafe.copyMemory(null, offset(j), null, offset(i), elementSpacing);
    unsafe.copyMemory(tmp, firstFieldOffset, null, offset(j), elementSize);
  }

  @Override public void add(int index, T element) {
    checkBoundsForAdd(index);
    Preconditions.checkNotNull(element);

    ensureCapacity(size + 1);

    if (elementSpacing < 8) {
      // TODO(bramp) remove this check
      // Note: A copyMemory copies in 8,4,2 or 1 byte chunks.
      throw new IllegalStateException("Moving data down is only supported when elementSize >= 8");
    }

    // Move data up to make room
    if (index < size) {
      unsafe.copyMemory(null, offset(index), null, offset(index + 1), elementSpacing * (size - index));
    }

    setNoCheck(index, element);
    size++;
  }

  @Override public T remove(int index) {
    T obj = get(index);

    size--;
    // Move everything down
    if (index < size) {
      unsafe.copyMemory(null, offset(index + 1), null, offset(index), elementSpacing * (size - index));
    }

    return obj;
  }

  @Override public int size() {
    return size;
  }

  /**
   * @return The size of this object in bytes
   */
  public long bytes() {
    return UnsafeHelper.sizeOf(this) + elementSpacing * capacity;
  }
}
