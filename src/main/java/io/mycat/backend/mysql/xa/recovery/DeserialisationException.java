package io.mycat.backend.mysql.xa.recovery;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class DeserialisationException extends Exception{
    private static final long serialVersionUID = -3835526236269555460L;

    public DeserialisationException(String content) {
        super(content);
    }
}
