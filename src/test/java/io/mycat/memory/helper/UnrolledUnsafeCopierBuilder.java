package io.mycat.memory.helper;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import sun.misc.Unsafe;

import java.lang.reflect.InvocationTargetException;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * This constructs a class specialised for copying the exact number of bytes specified.
 * It uses runtime byte code generation to unsafe a copy loop, for optimal performance.
 * The unrolled code looks like:
 * <pre>
 *   unsafe.putLong(dest, destOffset, unsafe.getLong(srcAddress));
 *   destOffset += 8; srcAddress += 8
 *   ...
 *   unsafe.putLong(dest, destOffset, unsafe.getLong(srcAddress));
 * </pre>
 */
public class UnrolledUnsafeCopierBuilder {

  long offset = -1;
  long length = -1;

  public UnrolledUnsafeCopierBuilder() {
  }

  public UnrolledUnsafeCopierBuilder of(Class clazz) {

    // TODO Check if the class has any non-primitive fields. If so, throw an exception.
    // new RuntimeException("Storing classes which contain references is dangerous, as the garbage collector will lose track of them thus is not supported")

    offset = UnsafeHelper.firstFieldOffset(clazz);
    length = UnsafeHelper.sizeOf(clazz) - offset;
    return this;
  }

  /**
   * Offset to the first field to copy.
   * @param offset
   * @return
   */
  public UnrolledUnsafeCopierBuilder offset(long offset) {
    this.offset = offset;
    return this;
  }

  /**
   * Length of memory to copy. Typically the size of all the packed fields.
   * @param length
   * @return
   */
  public UnrolledUnsafeCopierBuilder length(long length) {
    this.length = length;
    return this;
  }

  /**
   * Constructs a new Copier using the passed in Unsafe instance
   *
   * @param unsafe The sun.misc.Unsafe instance this copier uses
   * @return The new UnsageCopier built with the specific parameters
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException  if any argument is invalid
   */
  public UnsafeCopier build(Unsafe unsafe)
      throws IllegalAccessException, InstantiationException, NoSuchMethodException,
      InvocationTargetException {


    Class<?> dynamicType = new ByteBuddy()
        .subclass(UnsafeCopier.class)
        .method(named("copy"))
        .intercept(new CopierImplementation(offset, length)).make()
        .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
        .getLoaded();

    return (UnsafeCopier) dynamicType.getDeclaredConstructor(Unsafe.class).newInstance(unsafe);
  }
}
