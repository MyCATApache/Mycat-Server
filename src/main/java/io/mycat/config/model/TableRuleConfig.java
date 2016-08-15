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
package io.mycat.config.model;

import java.beans.Expression;

/**
 * @author mycat
 */
public final class TableRuleConfig {

    private final String name;
    private final RuleConfig[] rules;

    public TableRuleConfig(String name, RuleConfig[] rules) {
        this.name = name;
        this.rules = rules;
        if (rules != null) {
            for (RuleConfig r : rules) {
                r.tableRuleName = name;
            }
        }
    }

    public String getName() {
        return name;
    }

    public RuleConfig[] getRules() {
        return rules;
    }

    public static final class RuleConfig {
        private String tableRuleName;
        /** upper-case */
        private final String[] columns;
        private final Expression algorithm;

        public RuleConfig(String[] columns, Expression algorithm) {
            this.columns = columns == null ? new String[0] : columns;
            this.algorithm = algorithm;
        }

        public String[] getColumns() {
            return columns;
        }

        public Expression getAlgorithm() {
            return algorithm;
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();
            s.append("{tableRule:").append(tableRuleName).append(", columns:[");
            for (int i = 0; i < columns.length; ++i) {
                if (i > 0) {
                    s.append(", ");
                }
                s.append(columns[i]);
            }
            s.append("]}");
            return s.toString();
        }
    }

}