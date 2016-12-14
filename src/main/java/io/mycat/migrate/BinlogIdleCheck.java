package io.mycat.migrate;

import java.util.Date;

/**
 * Created by magicdoom on 2016/12/14.
 */
public class BinlogIdleCheck implements Runnable {
    private BinlogStream binlogStream;

    public BinlogIdleCheck(BinlogStream binlogStream) {
        this.binlogStream = binlogStream;
    }

    @Override public void run() {
//        Date lastDate=binlogStream.getLastDate();
//        long diff = (new Date().getTime() - lastDate.getTime())/1000;
//        if(diff>60){
//            //暂定60秒空闲 则代表增量任务结束，开始切换
//
//        }
    }


}
