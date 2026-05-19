/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.sftp.system;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.jcraft.jsch.ChannelSftp;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;

class SftpFileSystemTest {

    private static final class FakeChannelSftp extends ChannelSftp {}

    @Test
    void convertAllTypeFileName() {
        SFTPFileSystem sftpFileSystem = new SFTPFileSystem();
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e2e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e2e.txt"));
        // test file name with wildcard '*'
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e\\*e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e*e.txt"));

        // test file name with wildcard '?'
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e\\?e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e?e.txt"));
    }

    @Test
    void initializeShouldCreateConnectionPoolWithZeroLiveConnections() throws Exception {
        SFTPFileSystem sftpFileSystem = new SFTPFileSystem();
        Configuration configuration = new Configuration(false);
        configuration.setInt(SFTPFileSystem.FS_SFTP_CONNECTION_MAX, 7);

        sftpFileSystem.initialize(new URI("sftp://root:pwd@127.0.0.1/root"), configuration);

        Field connectionPoolField = SFTPFileSystem.class.getDeclaredField("connectionPool");
        connectionPoolField.setAccessible(true);
        SFTPConnectionPool connectionPool =
                (SFTPConnectionPool) connectionPoolField.get(sftpFileSystem);

        Assertions.assertNotNull(connectionPool);
        Assertions.assertEquals(7, connectionPool.getMaxConnection());
        Assertions.assertEquals(0, connectionPool.getLiveConnCount());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getFromPoolShouldNotDropOtherIdleConnectionsWithSameConnectionInfo() throws Exception {
        SFTPConnectionPool connectionPool = new SFTPConnectionPool(5);
        ChannelSftp channel1 = new FakeChannelSftp();
        ChannelSftp channel2 = new FakeChannelSftp();
        SFTPConnectionPool.ConnectionInfo connectionInfo =
                new SFTPConnectionPool.ConnectionInfo("127.0.0.1", 22, "root");

        Field con2infoMapField = SFTPConnectionPool.class.getDeclaredField("con2infoMap");
        con2infoMapField.setAccessible(true);
        HashMap<ChannelSftp, SFTPConnectionPool.ConnectionInfo> con2infoMap =
                (HashMap<ChannelSftp, SFTPConnectionPool.ConnectionInfo>) con2infoMapField.get(connectionPool);
        con2infoMap.put(channel1, connectionInfo);
        con2infoMap.put(channel2, connectionInfo);

        connectionPool.setMaxConnection(5);
        Field liveConnectionCountField =
                SFTPConnectionPool.class.getDeclaredField("liveConnectionCount");
        liveConnectionCountField.setAccessible(true);
        liveConnectionCountField.setInt(connectionPool, 2);

        connectionPool.returnToPool(channel1);
        connectionPool.returnToPool(channel2);

        Assertions.assertEquals(2, connectionPool.getIdleCount());

        ChannelSftp borrowed1 = connectionPool.getFromPool(connectionInfo);
        Assertions.assertNotNull(borrowed1);
        Assertions.assertEquals(1, connectionPool.getIdleCount());

        ChannelSftp borrowed2 = connectionPool.getFromPool(connectionInfo);
        Assertions.assertNotNull(borrowed2);
        Assertions.assertNotSame(borrowed1, borrowed2);
        Assertions.assertEquals(0, connectionPool.getIdleCount());
    }
}
