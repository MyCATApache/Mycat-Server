package io.mycat.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ConcurrentModificationException;

/**
 * Selector工具类
 * Created by Hash Zhang on 2017/7/24.
 */
public class SelectorUtil {
    private static final Logger logger = LoggerFactory.getLogger(SelectorUtil.class);

    public static final int REBUILD_COUNT_THRESHOLD = 512;

    public static final long MIN_SELECT_TIME_IN_NANO_SECONDS = 500000L;

    public static Selector rebuildSelector(final Selector oldSelector) throws IOException {
        final Selector newSelector;
        try {
            newSelector = Selector.open();
        } catch (Exception e) {
            logger.warn("Failed to create a new Selector.", e);
            return null;
        }

        int nChannels = 0;
        for (;;) {
            try {
                for (SelectionKey key: oldSelector.keys()) {
                    Object a = key.attachment();
                    try {
                        if (!key.isValid() || key.channel().keyFor(newSelector) != null) {
                            continue;
                        }
                        int interestOps = key.interestOps();
                        key.cancel();
                        key.channel().register(newSelector, interestOps, a);
                        nChannels ++;
                    } catch (Exception e) {
                        logger.warn("Failed to re-register a Channel to the new Selector.", e);
                    }
                }
            } catch (ConcurrentModificationException e) {
                // Probably due to concurrent modification of the key set.
                continue;
            }
            break;
        }
        oldSelector.close();
        return newSelector;
    }
}
