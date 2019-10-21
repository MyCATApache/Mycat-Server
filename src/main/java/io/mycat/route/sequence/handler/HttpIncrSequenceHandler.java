package io.mycat.route.sequence.handler;

import io.mycat.route.util.PropertiesUtil;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class HttpIncrSequenceHandler implements SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(HttpIncrSequenceHandler.class);
    private final String url;
    private long count = 0;
    private long limit = -1;

    public HttpIncrSequenceHandler() {
        Properties props = PropertiesUtil.loadProps("sequence_http_conf.properties");
        this.url = (String) props.get("url");
    }

    @Override
    public synchronized long nextId(String prefixName) {
        if (count > limit) {
            try {
                OkHttpClient client = new OkHttpClient.Builder()
                        .build();
                RequestBody body = new FormBody.Builder()
                        .add("prefixName", prefixName)
                        .add("count",Long.toString(count))
                        .add("limit", Long.toString(limit))
                        .build();
                Request request = new Request.Builder()
                        .url(url)
                        .post(body)
                        .build();
                final Call call = client.newCall(request);
                Response response = call.execute();
                ResponseBody body1 = response.body();
                String s = new String(body1.bytes());
                String[] split = s.split(",");
                this.count = Long.parseLong(split[0]);
                this.limit = Long.parseLong(split[1]);
            } catch (Throwable e) {
                LOGGER.error("", e);
                throw new RuntimeException(e);
            }
        }
        return count++;
    }
}