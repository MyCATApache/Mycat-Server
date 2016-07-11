package io.mycat.memory.unsafe.utils.sort;


import io.mycat.memory.unsafe.row.StructType;

/**
 * Created by zagnix on 2016/6/6.
 */
public final class SortPrefixUtils {
    public static boolean canSortFullyWithPrefix(long apply) {
        return true;
    }

    public static PrefixComparator getPrefixComparator(StructType keySchema) {
        return null;
    }

    public static UnsafeExternalRowSorter.PrefixComputer createPrefixGenerator(StructType keySchema) {
        return null;
    }

    /**
     * A dummy prefix comparator which always claims that prefixes are equal. This is used in cases
     * where we don't know how to generate or compare prefixes for a SortOrder.
     */
    private class NoOpPrefixComparator extends PrefixComparator {

        @Override
        public int compare(long prefix1, long prefix2) {
            return 0;
        }
    }
}
