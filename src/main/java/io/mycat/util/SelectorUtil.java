package io.mycat.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static Selector rebuildSelector(final Selector oldSelector) {
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
    }
}
