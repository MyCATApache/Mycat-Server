package io.mycat.route.util;

/**
 * Created by 862911 on 2016/5/4.
 */
public class CacheUtil {
        private static final int RUNS = 10;
        private static final int DIMENSION_1 = 1024 * 1024;
        private static final int DIMENSION_2 = 6;

        private static long[][] longs;

        public static void main(String[] args) throws Exception {
            Thread.sleep(10000);
            longs = new long[DIMENSION_1][];
            for (int i = 0; i < DIMENSION_1; i++) {
                longs[i] = new long[DIMENSION_2];
                for (int j = 0; j < DIMENSION_2; j++) {
                    longs[i][j] = 0L;
                }
            }
            System.out.println("starting....");

            long sum = 0L;
            for (int r = 0; r < RUNS; r++) {

                final long start = System.nanoTime();

//                slow
            for (int j = 0; j < DIMENSION_2; j++) {
                for (int i = 0; i < DIMENSION_1; i++) {
                    sum += longs[i][j];
                }
            }

                //fast
//                for (int i = 0; i < DIMENSION_1; i++) {
//                    for (int j = 0; j < DIMENSION_2; j++) {
//                        sum += longs[i][j];
//                    }
//                }

                System.out.println((System.nanoTime() - start));
            }

        }

}
