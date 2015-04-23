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
package org.opencloudb.sequence.handler;

import java.util.Map;

/**
 * zookeeper 实现递增序列号
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @time Create on 2013-12-29 下午11:04:47
 * @version 1.0
 */
public class IncrSequenceZKHandler extends IncrSequenceHandler {

	@Override
	public Map<String, String> getParaValMap(String prefixName) {
		
		return null;
	}

	@Override
	public Boolean fetchNextPeriod(String prefixName) {
		
		return null;
	}

	@Override
	public Boolean updateCURIDVal(String prefixName, Long val) {
		
		return null;
	}

}