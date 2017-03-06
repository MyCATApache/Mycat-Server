package io.mycat.util;

import io.mycat.migrate.MigrateUtils;
import io.mycat.util.dataMigrator.DataMigratorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

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
    public static String execReturnString(List<String> cmd) {
        Process process = null;
        try {
            //            Runtime runtime = Runtime.getRuntime();
            //            process = runtime.exec(cmd);
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            process=pb.start();
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

    public static void main(String[] args) {


//        List<String> argss= Arrays.asList("mysqldump", "-h127.0.0.1", "-P3301", "-uczn",
//                "-p123", "base1","test", "--single-transaction","-q","--default-character-set=utf8mb4","--hex-blob","--where=(_slot>=100 and _slot<=1000) or (_slot>=2000 and _slot <=100000)", "--master-data=1","-Tc:\\999"
//        ,"--fields-enclosed-by=\\\"","--fields-terminated-by=,", "--lines-terminated-by=\\n",  "--fields-escaped-by=\\\\");
//        String result=  ProcessUtil.execReturnString(argss);
//        System.out.println(result);

    }


}
