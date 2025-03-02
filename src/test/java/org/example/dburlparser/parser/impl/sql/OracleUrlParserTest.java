package org.example.dburlparser.parser.impl.sql;

import org.example.dburlparser.model.DbType;
import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.model.HostInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OracleUrlParserTest {
    private static final int PORT = 1521;
    private final OracleUrlParser parser = new OracleUrlParser();

    @Test
    public void testParseSidFormatUrl() {
        String url = "jdbc:oracle:thin:@localhost:1521:orcl";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.ORACLE, List.of(new HostInfo("localhost", PORT)), "orcl", Map.of());
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseServiceNameFormatUrl() {
        String url = "jdbc:oracle:thin:@//localhost:1521/service";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.ORACLE, List.of(new HostInfo("localhost", PORT)), "service", Map.of());
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseTnsFormatUrl() {
        String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=service)))";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.ORACLE, List.of(new HostInfo("localhost", PORT)), "service", Map.of());
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseTnsFormatWithMultipleHosts() {
        String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=host3)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=myservice)))";
        List<HostInfo> expectedHosts = List.of(
                new HostInfo("host1", PORT),
                new HostInfo("host2", PORT),
                new HostInfo("host3", PORT)
        );
        DbConnectionInfo expected = new DbConnectionInfo(DbType.ORACLE, expectedHosts, "myservice", Map.of());
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseTnsFormatWithProperties() {
        String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=myservice)))?ReadTimeout=30000&V8Compatible=true&oracle.net.CONNECT_TIMEOUT=5000&TcpNoDelay=true&useFetchSizeWithLongColumn=true&autoCommitSpecCompliant=false&defaultRowPrefetch=100";
        DbConnectionInfo expected = getDbConnectionInfo();
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    private static DbConnectionInfo getDbConnectionInfo() {
        List<HostInfo> expectedHosts = List.of(
                new HostInfo("host1", PORT),
                new HostInfo("host2", PORT)
        );
        Map<String, String> expectedProperties = Map.of(
                "ReadTimeout", "30000",
                "V8Compatible", "true",
                "oracle.net.CONNECT_TIMEOUT", "5000",
                "TcpNoDelay", "true",
                "useFetchSizeWithLongColumn", "true",
                "autoCommitSpecCompliant", "false",
                "defaultRowPrefetch", "100"
        );
        return new DbConnectionInfo(DbType.ORACLE, expectedHosts, "myservice", expectedProperties);
    }

    @Test
    void testParseServiceNameFormatWithMultipleHostsAndParams() {
        String url = "jdbc:oracle:thin:@//host1:1521,host2:1522,host3:1523/orclservice" +
                "?param1=value1&param2=value2&param3=value3";

        DbConnectionInfo expected = new DbConnectionInfo(
                DbType.ORACLE,
                List.of(
                        new HostInfo("host1", PORT),
                        new HostInfo("host2", 1522),
                        new HostInfo("host3", 1523)
                ),
                "orclservice",
                Map.of(
                        "param1", "value1",
                        "param2", "value2",
                        "param3", "value3"
                )
        );

        DbConnectionInfo actual = parser.parse(url);

        assertEquals(expected, actual);
    }

    @Test
    public void testParseInvalidUrl() {
        String url = "jdbc:oracle:thin:@localhost";
        assertThrows(IllegalArgumentException.class, () -> parser.parse(url));
    }

    @Test
    public void testParseMalformedTnsFormatUrl() {
        String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)))";
        assertThrows(IllegalArgumentException.class, () -> parser.parse(url));
    }
}


