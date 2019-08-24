/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.loadbalance;

import io.mycat.backend.datasource.PhysicalDatasource;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class LeastActiveLoadBalance implements LoadBalance {

    @Override
    public PhysicalDatasource doSelect(String hostName, ArrayList<PhysicalDatasource> okSources) {
        boolean sameWeight = true;
        int length = okSources.size();
        int[] leastIndexes = new int[length];
        int leastActive = -1;
        int leastCount = 0;
        int[] weights = new int[length];
        int totalWeight = 0;
        int firstWeight = 0;

        for (int i = 0; i < length; i++) {
            PhysicalDatasource okSource = okSources.get(i);
            int active = okSource.getActiveCount();
            int weight = okSource.getConfig().getWeight();
            if (weight == 0) {
                continue;
            }
            weights[i] = weight;
            if (leastActive == -1 || active < leastActive) {
                sameWeight = true;
                leastIndexes[0] = i;
                leastActive = active;
                leastCount = 1;
                totalWeight = weight;
                firstWeight = weight;
            } else if (active == leastActive) {
                leastIndexes[leastCount++] = i;
                totalWeight += weight;
                if (sameWeight && i > 0 && weight != firstWeight) {
                    sameWeight = false;
                }
            }
        }

        if (leastCount == 0) {
            return okSources.get(ThreadLocalRandom.current().nextInt(okSources.size()));
        }

        if (leastCount == 1) {
            return okSources.get(leastIndexes[0]);
        }

        if (!sameWeight && totalWeight > 0) {
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return okSources.get(leastIndex);
                }
            }
        }

        return okSources.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }

}