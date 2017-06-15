package debugger;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * 堆外内存测试
 * -XX:MaxDirectMemorySize=64m
 *
 * Created by yunai on 2017/6/14.
 */
public class OffHeapTest {

//    public static void main(String[] args) {
//        System.out.println(sun.misc.VM.maxDirectMemory());
//        ByteBuffer bb = ByteBuffer.allocateDirect(64 * 1024 * 1024);
////        bb = null; // 制空才会回收
//        ((DirectBuffer)bb).cleaner().clean();
////        System.gc();
//
//
//        main2(args);
//    }

    private static Unsafe getUnsafeInstance() throws SecurityException,
            NoSuchFieldException, IllegalArgumentException,
            IllegalAccessException {
        Field theUnsafeInstance = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafeInstance.setAccessible(true);
        return (Unsafe) theUnsafeInstance.get(Unsafe.class);
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        Unsafe unsafe = getUnsafeInstance();
        long s = unsafe.allocateMemory(64 * 1024 * 1024);

        main2(args);

        System.out.println(s);
    }

    public static void main2(String[] args) {

        ByteBuffer bb2 = ByteBuffer.allocateDirect(64 * 1024 * 1024);

        Collections.sort(new ArrayList<Comparable>(), new Comparator<Comparable>() {
            @Override
            public int compare(Comparable o1, Comparable o2) {
                return 0;
            }
        });
    }

}
