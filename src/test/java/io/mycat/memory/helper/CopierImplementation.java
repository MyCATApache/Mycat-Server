package io.mycat.memory.helper;


import io.mycat.memory.helper.bytebuddy.LongAdd;
import io.mycat.memory.helper.bytebuddy.MethodVariableStore;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.ParameterList;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.constant.LongConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.MethodVisitor;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * | 0 1 2 3 4 6 7 8 0 1 2 3 4 6 7 8
 * |     s s s s
 * |                 d d d d
 * 0x00000000: XX XX 00 00 00 00 00 00
 * 0x00000000: C1 0C 65 DF XX XX 00 00
 *
 * </pre>
 */
class CopierImplementation implements ByteCodeAppender, Implementation {

  public static final long COPY_STRIDE = 8;

  final long offset;
  final long length;

  /**
   * Creates a CopierImplementation.
   *
   * @param offset offset in destination object to start copying to
   * @param length number of bytes to copy
   */
  public CopierImplementation(long offset, long length) {
    this.offset = offset;
    this.length = length;



    // On intel we can do unaligned reads, but perhaps fix this on other platforms
    //Preconditions
    //    .checkArgument(offset % COPY_STRIDE == 0, "We only support destination offsets aligned to 8 bytes");
    //Preconditions
    //    .checkArgument(length % COPY_STRIDE == 0, "We only support lengths multiple of 8 bytes");
  }

  private void buildSetupStack(List<StackManipulation> stack) throws NoSuchFieldException, NoSuchMethodException {

    final StackManipulation setupStack = new StackManipulation.Compound(
        LongConstant.forValue(offset),           // LDC offset
        MethodVariableStore.LONG.storeOffset(4)  // LSTORE 4
    );

    stack.add(setupStack);
  }

  private void buildCopyStack(List<StackManipulation> stack, int iterations, Method getMethod, Method putMethod, long stride) throws
      NoSuchFieldException, NoSuchMethodException {

    final Field unsafeField = UnsafeCopier.class.getDeclaredField("unsafe");

    final StackManipulation copyStack = new StackManipulation.Compound(
        // unsafe.putLong(dest, destOffset, unsafe.getLong(src));
        MethodVariableAccess.REFERENCE.loadOffset(0), // ALOAD 0 this

        FieldAccess.forField(new FieldDescription.ForLoadedField(unsafeField)).getter(), // GETFIELD

        MethodVariableAccess.REFERENCE.loadOffset(1), // ALOAD 1 dest
        MethodVariableAccess.LONG.loadOffset(4),      // LLOAD 4 destOffset

        MethodVariableAccess.REFERENCE.loadOffset(0), // ALOAD 0 this
        FieldAccess.forField(new FieldDescription.ForLoadedField(unsafeField)).getter(), // GETFIELD

        MethodVariableAccess.LONG.loadOffset(2),      // LLOAD 2 src

        MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(getMethod)),
        MethodInvocation.invoke(new MethodDescription.ForLoadedMethod(putMethod))
    );

    final StackManipulation incrementStack = new StackManipulation.Compound(
        // destOffset += 8; src += 8;
        MethodVariableAccess.LONG.loadOffset(4), // LLOAD 4 destOffset
        LongConstant.forValue(stride),      // LDC 8 strideWidth
        LongAdd.INSTANCE,                        // LADD
        MethodVariableStore.LONG.storeOffset(4), // LSTORE 4

        MethodVariableAccess.LONG.loadOffset(2), // LLOAD 2 src
        LongConstant.forValue(stride),      // LDC 8 strideWidth
        LongAdd.INSTANCE,                        // LADD
        MethodVariableStore.LONG.storeOffset(2)  // LSTORE 2
    );

    for (int i = 0; i < iterations; i++) {
      stack.add(copyStack);
      stack.add(incrementStack);
    }
  }

  /**
   * Creates iterations copies of the equivalent java code
   * <pre>
   * unsafe.putLong(dest, destOffset, unsafe.getLong(src));
   * destOffset += COPY_STRIDE; src += COPY_STRIDE
   * </pre>
   *
   * @param iterations
   * @throws NoSuchFieldException
   * @throws NoSuchMethodException
   */
  private void buildLongCopyStack(List<StackManipulation> stack, int iterations) throws NoSuchFieldException, NoSuchMethodException {
    final Method getLongMethod = Unsafe.class.getMethod("getLong", long.class);
    final Method putLongMethod = Unsafe.class.getMethod("putLong", Object.class, long.class, long.class);

    buildCopyStack(stack, iterations, getLongMethod, putLongMethod, COPY_STRIDE);
  }

  /**
   * Creates iterations copies of the equivalent java code
   * <pre>
   * unsafe.putByte(dest, destOffset, unsafe.getByte(src));
   * destOffset += 1; src += 1
   * </pre>
   *
   * @param iterations
   * @throws NoSuchFieldException
   * @throws NoSuchMethodException
   */
  private void buildByteCopyStack(List<StackManipulation> stack, int iterations) throws NoSuchFieldException, NoSuchMethodException {
    final Method getByteMethod = Unsafe.class.getMethod("getByte", long.class);
    final Method putByteMethod = Unsafe.class.getMethod("putByte", Object.class, long.class, byte.class);

    buildCopyStack(stack, iterations, getByteMethod, putByteMethod, 1);
  }

  private static StackManipulation toStackManipulation(List<StackManipulation> stack) {
    return new StackManipulation.Compound(stack.toArray(new StackManipulation[stack.size()]));
  }

  private StackManipulation buildStack() throws NoSuchFieldException, NoSuchMethodException {

    if (length == 0) {
      return MethodReturn.VOID;
    }

    final int remainder = (int) (length % COPY_STRIDE);
    final int iterations = (int) ((length - remainder) / COPY_STRIDE);


    // Construct a sequence of stack manipulations
    List<StackManipulation> stack = new ArrayList<>();

    buildSetupStack(stack);

    if (iterations > 0) {
      buildLongCopyStack(stack, iterations);

      if (remainder == 0) {
        // The last increment is not needed
        stack.remove(stack.size() - 1);
      }
    }

    if (remainder > 0) {
      // Do a couple of more byte by byte copies
      buildByteCopyStack(stack, remainder);

      // The last increment is not needed
      stack.remove(stack.size() - 1);
    }

    stack.add(MethodReturn.VOID);

    return toStackManipulation(stack);
  }

  private void checkMethodSignature(MethodDescription instrumentedMethod) {
    final String errMessage = "%s must have signature `void copy(java.lang.Object, long)`";
    Preconditions.checkArgument(instrumentedMethod.getReturnType().represents(void.class),
        errMessage, instrumentedMethod);

    ParameterList parameters = instrumentedMethod.getParameters();
    Preconditions.checkArgument(parameters.size() == 2, errMessage, instrumentedMethod);

    Preconditions.checkArgument(parameters.get(0).getTypeDescription().represents(Object.class),
        errMessage, instrumentedMethod);

    Preconditions.checkArgument(parameters.get(1).getTypeDescription().represents(long.class),
        errMessage, instrumentedMethod);
  }

  public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext,
                    MethodDescription instrumentedMethod) {

    checkMethodSignature(instrumentedMethod);

    try {
      StackManipulation stack = buildStack();
      StackManipulation.Size finalStackSize = stack.apply(methodVisitor, implementationContext);

      return new Size(finalStackSize.getMaximalSize(),
          instrumentedMethod.getStackSize() + 2); // 2 stack slots for a single local variable

    } catch (NoSuchMethodException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  public InstrumentedType prepare(InstrumentedType instrumentedType) {
    return instrumentedType;
  }

  public ByteCodeAppender appender(Target implementationTarget) {
    return this;
  }
}
