/**
 * Copyright (C) <2019>  <mycat>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.server;

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * ERROR Packet接口
 **/
public interface ErrorPacket {

    byte SQLSTATE_MARKER = (byte) '#';
    byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

    int getErrorStage();

    void setErrorStage(int stage);

    int getErrorMaxStage();

    void setErrorMaxStage(int maxStage);

    int getErrorProgress();

    void setErrorProgress(int progress);

    byte[] getErrorProgressInfo();

    void setErrorProgressInfo(byte[] progress_info);

    byte getErrorMark();

    void setErrorMark(byte mark);

    byte[] getErrorSqlState();

    void setErrorSqlState(byte[] sqlState);

    byte[] getErrorMessage();
    default String getErrorMessageString(){
        return new String(getErrorMessage());
    }

    void setErrorMessage(byte[] message);

}
