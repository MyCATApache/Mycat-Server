package org.opencloudb.mpp.tmp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Ordering;

/**
 * 
 * from http://www.brilliantsheep.com/java-implementation-of-hoares-selection-algorithm-quickselect/
 */
public class QuickSelect {

    /** Helper method for select( int[ ], int, int, int ) */
    public static int select(Integer[] array, int k) {
        return select(array, 0, array.length - 1, k);
    }

    /**
     * Returns the value of the kth lowest element.
     * Do note that for nth lowest element, k = n - 1.
     */
    private static int select(Integer[] array, int left, int right, int k) {

        while (true) {

            if (right <= left + 1) {

                if (right == left + 1 && array[right] < array[left]) {
                    swap(array, left, right);
                }

                return array[k];

            } else {

                int middle = (left + right) >>> 1;
                swap(array, middle, left + 1);

                if (array[left] > array[right]) {
                    swap(array, left, right);
                }

                if (array[left + 1] > array[right]) {
                    swap(array, left + 1, right);
                }

                if (array[left] > array[left + 1]) {
                    swap(array, left, left + 1);
                }

                int i = left + 1;
                int j = right;
                int pivot = array[left + 1];

                while (true) {
                    do
                        i++;
                    while (array[i] < pivot);
                    do
                        j--;
                    while (array[j] > pivot);

                    if (j < i) {
                        break;
                    }

                    swap(array, i, j);
                }

                array[left + 1] = array[j];
                array[j] = pivot;

                if (j >= k) {
                    right = j - 1;
                }

                if (j <= k) {
                    left = i;
                }
            }
        }
    }

    /** Helper method for swapping array entries */
    private static void swap(Integer[] array, int a, int b) {
        int temp = array[a];
        array[a] = array[b];
        array[b] = temp;
    }

    /** Test method */
    public static void main(String[] args) {
        Set<Integer> set = new HashSet<Integer>();
        int dataCount = 1000000;
        Random rd = new Random();
        int bound = dataCount * 3;
        while (set.size() < dataCount) {
            set.add(rd.nextInt(bound));
        }
        int topN = 50;
        Integer[] input = set.toArray(new Integer[dataCount]);
        long st = System.currentTimeMillis();
        List<Integer> q = Ordering.natural().greatestOf(set.iterator(), topN);
//        for (int i = 0; i < topN; i++) {
//            System.out.print(q.get(i)+",");
//        }
//        System.out.println();
        System.out.println("Google1->count:"+dataCount +" top:"+topN+" time:" + (System.currentTimeMillis() - st) / 1000.0);
        int[] datax = new int[topN];
        for (int i = 0; i < datax.length; i++) {
            datax[i] = select(input, i);
        }
//        System.out.println(Arrays.toString(datax));
        System.out.println("Google->count:"+dataCount +" top:"+topN+" time:" + (System.currentTimeMillis() - st) / 1000.0);
        st = System.currentTimeMillis();
        int i = 0;
        int[] data = new int[topN];
        Iterator<Integer> it = set.iterator();
        while (i < topN) {
            data[i++] = it.next();
        }
        IntMinHeap heap = new IntMinHeap(data);
        heap.buildMinHeap();
        while (it.hasNext())
            heap.addIfRequired(it.next());
        Arrays.sort(data);
//        System.out.println(Arrays.toString(data));
        System.out.println("My->count:"+dataCount +" top:"+topN+" time:" + (System.currentTimeMillis() - st) / 1000.0);
    }
}