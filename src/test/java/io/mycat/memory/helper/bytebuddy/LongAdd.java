package io.mycat.memory.helper.bytebuddy;

import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;

public enum LongAdd implements StackManipulation {
 
  INSTANCE; // singleton
 
  public boolean isValid() {
    return true;
  }

  public Size apply(MethodVisitor methodVisitor,
                    Implementation.Context implementationContext) {
    methodVisitor.visitInsn(Opcodes.LADD);
    return new Size(-1, 0);
  }
}