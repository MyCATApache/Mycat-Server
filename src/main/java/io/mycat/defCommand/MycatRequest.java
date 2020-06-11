package io.mycat.defCommand;

/**
 * @author Junwen Chen
 **/
public class MycatRequest {
    final String text;
    public MycatRequest(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}