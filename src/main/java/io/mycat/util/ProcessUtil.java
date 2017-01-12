package io.mycat.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ProcessUtil
{
    private static Logger LOGGER = LoggerFactory.getLogger((ProcessUtil.class));



    public static int exec(String cmd) {
        Process process = null;
        try {
            Runtime runtime = Runtime.getRuntime();
            process = runtime.exec(cmd);
            new StreamGobble(process.getInputStream(), "INFO").start();
            new StreamGobble(process.getErrorStream(), "ERROR").start();
            return process.waitFor();
        } catch (Throwable t) {
           LOGGER.error(t.getMessage());
        } finally {
            if (process != null)
                process.destroy();

        }
        return 0;
    }

    public static String execReturnString(String cmd) {
        Process process = null;
        try {
            Runtime runtime = Runtime.getRuntime();
            process = runtime.exec(cmd);
            StreamGobble inputGobble = new StreamGobble(process.getInputStream(), "INFO");
            inputGobble.start();
            new StreamGobble(process.getErrorStream(), "ERROR").start();
             process.waitFor();
            return inputGobble.getResult();
        } catch (Throwable t) {
            LOGGER.error(t.getMessage());
        } finally {
            if (process != null)
                process.destroy();

        }
        return null;
    }
    public static int exec(String cmd,File dir) {
        Process process = null;
        try {
            Runtime runtime = Runtime.getRuntime();
            process = runtime.exec(cmd,null,dir);
            new StreamGobble(process.getInputStream(), "INFO").start();
            new StreamGobble(process.getErrorStream(), "ERROR").start();
            return process.waitFor();

        } catch (Throwable t) {
            LOGGER.error(t.getMessage());
        } finally {
            if (process != null)
                process.destroy();

        }
        return 0;
    }




}
