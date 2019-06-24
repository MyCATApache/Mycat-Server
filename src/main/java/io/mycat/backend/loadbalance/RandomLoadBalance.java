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

public class RandomLoadBalance implements LoadBalance {

    @Override
    public PhysicalDatasource doSelect(String hostName, ArrayList<PhysicalDatasource> okSources) {
        boolean sameWeight = true;
        int length = okSources.size();
        int[] weights = new int[length];
        int firstWeight = okSources.get(0).getConfig().getWeight();
        weights[0] = firstWeight;
        int totalWeight = firstWeight;

        for (int i = 1; i < length; i++) {
            int weight = okSources.get(i).getConfig().getWeight();
            weights[i] = weight;
            totalWeight += weight;
            if (sameWeight && weight != firstWeight) {
                sameWeight = false;
            }
        }

        if (!sameWeight && totalWeight > 0) {
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < length; i++) {
                offset -= weights[i];
                if (offset < 0) {
                    return okSources.get(i);
                }
            }
        }

        return okSources.get(ThreadLocalRandom.current().nextInt(length));
    }

}