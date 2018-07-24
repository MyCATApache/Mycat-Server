package io.mycat.util;

/**
 * ${DESCRIPTION}
 *
 * @author dengliaoyan
 * @create 2018/7/24
 */
public class ArrayUtil {
    public static boolean arraySearch(Object[] arrays, Object o){
        if(arrays==null || arrays.length==0)return false;
        if(arrays[0].getClass() != o.getClass())return false;
        for(Object item : arrays){
            if(item.equals(o)){
                return true;
            }
        }
        return false;
    }
}
