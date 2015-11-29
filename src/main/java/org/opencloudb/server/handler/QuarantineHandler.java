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
package org.opencloudb.server.handler;

import org.opencloudb.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author songwie
 */
public final class QuarantineHandler {

	private static Logger logger = LoggerFactory.getLogger(QuarantineHandler.class);
	
	public static void handle(String sql, ServerConnection c) {
		/*MycatConfig config = MycatServer.getInstance().getConfig();
		QuarantineConfig qc = config.getQuarantine();
		WallProvider wall = qc.getWallProvider();
		WallCheckResult result = wall.check(sql);
		if (result.getViolations().size() > 0) {
            Violation violation = result.getViolations().get(0);
            logger.warn("found sql quarantine," + violation.getMessage());
            c.writeErrMessage(ErrorCode.ERR_WRONG_USED, "found sql quarantine," + violation.getMessage());
        }*/
		 
	}

}