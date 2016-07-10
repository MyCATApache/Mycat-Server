package io.mycat.memory.helper.bytebuddy;

import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackSize;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;

/**
 * A stack assignment that stores a method variable from a given index of the local variable array.
 *
 * <p>This class is a copy of MethodVariableLoader with a few things changed to make it suitable for
 * storing.
 */
public enum MethodVariableStore {

  /**
   * The accessor handler for a JVM-integer.
   */
  INTEGER(Opcodes.ISTORE, 5, StackSize.SINGLE),

  /**
   * The accessor handler for a {@code long}.
   */
  LONG(Opcodes.LSTORE, 8, StackSize.DOUBLE),

  /**
   * The accessor handler for a {@code float}.
   */
  FLOAT(Opcodes.FSTORE, 11, StackSize.SINGLE),

  /**
   * The accessor handler for a {@code double}.
   */
  DOUBLE(Opcodes.DSTORE, 14, StackSize.DOUBLE),

  /**
   * The accessor handler for a reference type.
   */
  REFERENCE(Opcodes.ASTORE, 17, StackSize.SINGLE);

  /**
   * The opcode for storing this variable.
   */
  private final int storeOpcode;

  /**
   * The offset for any shortcut opcode that allows to store a variable from a low range index,
   * such as {@code ASTORE_0}, {@code ISTORE_0} etc.
   */
  private final int storeOpcodeShortcutOffset;

  /**
   * The size impact of this stack manipulation.
   */
  private final StackManipulation.Size size;

  /**
   * Creates a new method variable access for a given JVM type.
   *
   * @param storeOpcode               The opcode for loading this variable.
   * @param storeOpcodeShortcutOffset The offset for any shortcut opcode that allows to load a
   *                                  variable from a low range index, such as {@code ASTORE_0},
   *                                  {@code ISTORE_0} etc.
   * @param stackSize                 The size of the JVM type.
   */
  MethodVariableStore(int storeOpcode, int storeOpcodeShortcutOffset, StackSize stackSize) {
    this.storeOpcode = storeOpcode;
    this.storeOpcodeShortcutOffset = storeOpcodeShortcutOffset;
    this.size = stackSize.toIncreasingSize();
  }

  /**
   * Locates the correct accessor for a variable of a given type.
   *
   * @param typeDescription The type of the variable to be loaded.
   * @return An accessor for the given type.
   */
  public static MethodVariableStore forType(TypeDescription typeDescription) {
    if (typeDescription.isPrimitive()) {
      if (typeDescription.represents(long.class)) {
        return LONG;
      } else if (typeDescription.represents(double.class)) {
        return DOUBLE;
      } else if (typeDescription.represents(float.class)) {
        return FLOAT;
      } else if (typeDescription.represents(void.class)) {
        throw new IllegalArgumentException("Variable type cannot be void");
      } else {
        return INTEGER;
      }
    } else {
      return REFERENCE;
    }
  }

  /**
   * Creates a stack assignment for a given index of the local variable array.
   *
   * <p>The index has to be relative to the method's local variable array size.
   *
   * @param variableOffset the offset of the variable where {@code double} and {@code long} types
   *                       count two slots.
   * @return a stack manipulation representing the method retrieval.
   */
  public StackManipulation storeOffset(int variableOffset) {
    return new ArgumentStoringStackManipulation(variableOffset);
  }

  @Override public String toString() {
    return "MethodVariableStore." + name();
  }

  /**
   * A stack manipulation for loading a variable of a method's local variable array onto the operand
   * stack.
   */
  protected class ArgumentStoringStackManipulation implements StackManipulation {

    /**
     * The index of the local variable array from which the variable should be loaded.
     */
    private final int variableIndex;

    /**
     * Creates a new argument loading stack manipulation.
     *
     * @param variableIndex the index of the local variable array from which the variable should be
     *                      stored.
     */
    protected ArgumentStoringStackManipulation(int variableIndex) {
      this.variableIndex = variableIndex;
    }

    public boolean isValid() {
      return true;
    }

    public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
      switch (variableIndex) {
        case 0:
          methodVisitor.visitInsn(storeOpcode + storeOpcodeShortcutOffset);
          break;
        case 1:
          methodVisitor.visitInsn(storeOpcode + storeOpcodeShortcutOffset + 1);
          break;
        case 2:
          methodVisitor.visitInsn(storeOpcode + storeOpcodeShortcutOffset + 2);
          break;
        case 3:
          methodVisitor.visitInsn(storeOpcode + storeOpcodeShortcutOffset + 3);
          break;
        default:
          methodVisitor.visitVarInsn(storeOpcode, variableIndex);
          break;
      }
      return size;
    }

    /**
     * Returns the outer instance.
     *
     * @return the outer instance.
     */
    private MethodVariableStore getMethodVariableAccess() {
      return MethodVariableStore.this;
    }

    @Override public boolean equals(Object other) {
      return this == other || !(other == null || getClass() != other.getClass())
          && MethodVariableStore.this == ((ArgumentStoringStackManipulation) other)
          .getMethodVariableAccess()
          && variableIndex == ((ArgumentStoringStackManipulation) other).variableIndex;
    }

    @Override public int hashCode() {
      return MethodVariableStore.this.hashCode() + 31 * variableIndex;
    }

    @Override public String toString() {
      return "MethodVariableStore.ArgumentStoringStackManipulation{"
          + "MethodVariableStore=" + MethodVariableStore.this
          + " ,variableIndex=" + variableIndex + '}';
    }
  }
}

