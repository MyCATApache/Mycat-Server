package io.mycat.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamGobble extends Thread {
    InputStream is;
    String type;
   private StringBuffer result=new StringBuffer();

    public String getResult() {
        return result.toString();
    }

    private static Logger LOG = LoggerFactory.getLogger((StreamGobble.class));

    StreamGobble(InputStream is, String type) {
        this.is = is;
        this.type = type;
    }

    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                result.append(line).append("\n");
                LOG.info(line);
            }
        } catch (IOException ioe) {
            LOG.error(ioe.getMessage());
        }
    }
}


