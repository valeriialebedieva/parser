package org.example.dburlparser.parser.impl.nosql;

import org.example.dburlparser.model.DbType;
import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.model.HostInfo;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CassandraUrlParserTest {
    private static final int PORT = 9042;
    private final CassandraUrlParser parser = new CassandraUrlParser();

    @Test
    void testSingleHostWithKeyspaceAndParams() {
        String url = "jdbc:cassandra://host1:9042/keyspace1?param1=value1&param2=value2";

        DbConnectionInfo expected = new DbConnectionInfo(
                DbType.CASSANDRA,
                List.of(new HostInfo("host1", PORT)),
                "keyspace1",
                Map.of("param1", "value1", "param2", "value2")
        );

        DbConnectionInfo actual = parser.parse(url);
        assertEquals(expected, actual);
    }

    @Test
    void testMultipleHostsWithKeyspaceAndParams() {
        String url = "jdbc:cassandra://host1:9042,host2:9043,host3/keyspace2?paramA=valA";

        DbConnectionInfo expected = new DbConnectionInfo(
                DbType.CASSANDRA,
                List.of(
                        new HostInfo("host1", PORT),
                        new HostInfo("host2", 9043),
                        new HostInfo("host3", PORT)
                ),
                "keyspace2",
                Map.of("paramA", "valA")
        );

        DbConnectionInfo actual = parser.parse(url);
        assertEquals(expected, actual);
    }

    @Test
    void testSingleHostWithKeyspaceNoParams() {
        String url = "jdbc:cassandra://host1/keyspace3";

        DbConnectionInfo expected = new DbConnectionInfo(
                DbType.CASSANDRA,
                List.of(new HostInfo("host1", PORT)),
                "keyspace3",
                Collections.emptyMap()
        );

        DbConnectionInfo actual = parser.parse(url);
        assertEquals(expected, actual);
    }

    @Test
    void testSingleHostWithParamsNoKeyspace() {
        String url = "jdbc:cassandra://host1:9044?timeout=30&ssl=true";
        assertThrows(IllegalArgumentException.class, () -> parser.parse(url));
    }

    @Test
    void testUrlWithUserInfoIgnored() {
        String url = "jdbc:cassandra://user:pass@host1:9042/keyspace4?compression=snappy";

        DbConnectionInfo expected = new DbConnectionInfo(
                DbType.CASSANDRA,
                List.of(new HostInfo("host1", PORT)),
                "keyspace4",
                Map.of("compression", "snappy")
        );

        DbConnectionInfo actual = parser.parse(url);
        assertEquals(expected, actual);
    }

    @Test
    void testComplexUrlWithMultipleHostsAndParams() {
        String url = "jdbc:cassandra://host1,host2,host3:9042/mykeyspace" +
                "?consistency=QUORUM" +
                "&localDatacenter=datacenter1" +
                "&readTimeoutMillis=5000" +
                "&connectTimeoutMillis=3000" +
                "&useSSL=true" +
                "&loadBalancing.localDc=datacenter1" +
                "&reconnectBaseDelay=1000" +
                "&reconnectMaxDelay=10000" +
                "&compression=SNAPPY" +
                "&fetchSize=5000";

        DbConnectionInfo expected = new DbConnectionInfo(
                DbType.CASSANDRA,
                List.of(
                        new HostInfo("host1", PORT),
                        new HostInfo("host2", PORT),
                        new HostInfo("host3", PORT)
                ),
                "mykeyspace",
                Map.of(
                        "consistency", "QUORUM",
                        "localDatacenter", "datacenter1",
                        "readTimeoutMillis", "5000",
                        "connectTimeoutMillis", "3000",
                        "useSSL", "true",
                        "loadBalancing.localDc", "datacenter1",
                        "reconnectBaseDelay", "1000",
                        "reconnectMaxDelay", "10000",
                        "compression", "SNAPPY",
                        "fetchSize", "5000"
                )
        );

        DbConnectionInfo actual = parser.parse(url);
        assertEquals(expected, actual);
    }

    @Test
    void testMissingHost() {
        String url = "jdbc:cassandra:///keyspace";
        assertThrows(IllegalArgumentException.class, () -> parser.parse(url));
    }

    @Test
    void testInvalidPort() {
        String url = "jdbc:cassandra://host1:invalidPort/keyspace";
        assertThrows(NumberFormatException.class, () -> parser.parse(url));
    }

}
