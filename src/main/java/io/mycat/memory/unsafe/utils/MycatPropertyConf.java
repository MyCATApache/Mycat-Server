package io.mycat.memory.unsafe.utils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zagnix on 2016/6/2.
 */
public class MycatPropertyConf {

    private ConcurrentHashMap settings = new ConcurrentHashMap<String, String>();

    public MycatPropertyConf(){

    }

    /** Set a configuration variable. */
    public MycatPropertyConf set(String key, String value) {
        set(key, value, false);
        return  this;
    }


    public MycatPropertyConf set(String key, String value, boolean silent){

        if (key == null) {
            throw new NullPointerException("null key");
        }
        if (value == null) {
            throw new NullPointerException("null value for " + key);
        }

        if (!silent) {
        }

        settings.put(key, value);
        return this;
    }

    public long getSizeAsBytes(String s, long i) {
        String value = (String) settings.get(s);
        if(value !=null){
            return byteStringAsBytes(value);
        }
        return i;
    }

    public long getSizeAsBytes(String s, String defaultValue) {
        String value = (String) settings.get(s);
        if(value !=null){
            return byteStringAsBytes(value);
        }
        return byteStringAsBytes(defaultValue);
    }


    public double getDouble(String s, double v) {
        return v;
    }

    public boolean getBoolean(String s, boolean b) {
        String value = (String) settings.get(s);
        if(value !=null){

            if(value.equals("true")){
                return true;
            }else{
                return false;
            }
        }
        return b;
    }


    public long getLong(String s, long l) {
        return l;
    }

    public boolean contains(String s) {
        return  true;
    }

    public int getInt(String s, int i) {
        return  i;
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in bytes.
     */
    public Long byteStringAsBytes(String str) {
       return JavaUtils.byteStringAsBytes(str);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in kibibytes.
     */
    public Long byteStringAsKb(String str){
       return JavaUtils.byteStringAsKb(str);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in mebibytes.
     */
    public Long byteStringAsMb(String str) {
       return JavaUtils.byteStringAsMb(str);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m, 500g) to gibibytes for internal use.
     *
     * If no suffix is provided, the passed number is assumed to be in gibibytes.
     */
    public Long byteStringAsGb(String str) {
        return  JavaUtils.byteStringAsGb(str);
    }

    /**
     * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of mebibytes.
     */
    public int memoryStringToMb(String str){
        // Convert to bytes, rather than directly to MB, because when no units are specified the unit
        // is assumed to be bytes
       return (int) (JavaUtils.byteStringAsBytes(str) / 1024 / 1024);
    }

    public String getString(String s, String defaultValue) {

        String value = (String) settings.get(s);
        if(value !=null){
            return value;
        }
        return defaultValue;
    }
}
