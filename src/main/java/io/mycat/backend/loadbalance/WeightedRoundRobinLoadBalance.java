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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class WeightedRoundRobinLoadBalance implements LoadBalance {

    private Map<String, Map<String, WeightedRoundRobin>> weightedRoundRobinMap = new ConcurrentHashMap<>();

    protected static class WeightedRoundRobin {

        private int weight;
        private AtomicInteger current = new AtomicInteger(0);

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }

        public int increaseCurrent() {
            return current.addAndGet(weight);
        }

        public void select(int total) {
            current.addAndGet(-1 * total);
        }

    }

    @Override
    public PhysicalDatasource doSelect(String hostName, ArrayList<PhysicalDatasource> okSources) {
        Map<String, WeightedRoundRobin> map = weightedRoundRobinMap.get(hostName);
        if (map == null) {
            Map<String, WeightedRoundRobin> newMap = new ConcurrentHashMap<>();
            map = weightedRoundRobinMap.putIfAbsent(hostName, newMap);
            if (map == null) {
                map = newMap;
            }
        }

        int totalWeight = 0;
        int maxCurrent = Integer.MIN_VALUE;
        PhysicalDatasource selectedOkSource = null;
        WeightedRoundRobin selectedWeightedRoundRobin = null;

        for (PhysicalDatasource okSource : okSources) {
            String name = okSource.getName();
            WeightedRoundRobin weightedRoundRobin = map.get(name);
            int weight = okSource.getConfig().getWeight();
            if (weight <= 0) {
                continue;
            }
            if (weightedRoundRobin == null) {
                weightedRoundRobin = new WeightedRoundRobin();
                weightedRoundRobin.setWeight(weight);
                map.putIfAbsent(name, weightedRoundRobin);
            }
            int current = weightedRoundRobin.increaseCurrent();
            if (current > maxCurrent) {
                maxCurrent = current;
                selectedOkSource = okSource;
                selectedWeightedRoundRobin = weightedRoundRobin;
            }
            totalWeight += weight;
        }

        if (selectedOkSource == null) {
            return okSources.get(ThreadLocalRandom.current().nextInt(okSources.size()));
        }

        selectedWeightedRoundRobin.select(totalWeight);
        return selectedOkSource;
    }

}