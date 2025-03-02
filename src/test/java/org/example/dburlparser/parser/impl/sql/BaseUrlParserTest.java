package org.example.dburlparser.parser.impl.sql;

import org.example.dburlparser.model.DbType;
import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.model.HostInfo;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class BaseUrlParserTest {

    private static final int PORT = 3306;
    private final BaseUrlParser parser = new BaseUrlParser();

    @Test
    public void testParseValidUrlSingleHostWithDatabase() {
        String url = "jdbc:mysql://localhost:3306/testdb";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.MYSQL, List.of(new HostInfo("localhost", PORT)), "testdb", Map.of());
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseValidUrlSingleHostWithoutDatabase() {
        String url = "jdbc:mysql://localhost:3306";
        assertThrows(IllegalArgumentException.class, () -> parser.parse(url));
    }

    @Test
    public void testParseValidUrlMultipleHosts() {
        String url = "jdbc:mysql://host1:3306,host2:3307/testdb";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.MYSQL, List.of(new HostInfo("host1", PORT), new HostInfo("host2", 3307)), "testdb", Map.of());
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseValidUrlWithProperties() {
        String url = "jdbc:mysql://localhost:3306/testdb?user=root&password=secret";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.MYSQL, List.of(new HostInfo("localhost", PORT)), "testdb", Map.of("user", "root", "password", "secret"));
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseValidUrlWithoutPort() {
        String url = "jdbc:mysql://localhost/testdb";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.MYSQL, List.of(new HostInfo("localhost", PORT)), "testdb", Map.of());
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseInvalidUrlEmptyHost() {
        String url = "jdbc:mysql:///testdb";
        assertThrows(IllegalArgumentException.class, () -> parser.parse(url));
    }

    @Test
    public void testParseInvalidUrlMalformedQuery() {
        String url = "jdbc:mysql://localhost:3306/testdb?user=root&password";
        DbConnectionInfo expected = new DbConnectionInfo(DbType.MYSQL,
                List.of(new HostInfo("localhost", PORT)), "testdb", Map.of("user", "root"));
        DbConnectionInfo result = parser.parse(url);

        assertEquals(expected, result);
    }

    @Test
    public void testParseUrlWithAllPossibleParameters() {
        String url = "jdbc:mysql://host1:3306,host2:3306,host3:3306/mydatabase" +
                "?user=myuser" +
                "&password=mypassword" +
                "&useSSL=true" +
                "&requireSSL=true" +
                "&verifyServerCertificate=false" +
                "&autoReconnect=true" +
                "&failOverReadOnly=false" +
                "&allowPublicKeyRetrieval=true" +
                "&serverTimezone=UTC" +
                "&characterEncoding=UTF-8" +
                "&useUnicode=true" +
                "&connectTimeout=5000" +
                "&socketTimeout=10000" +
                "&rewriteBatchedStatements=true" +
                "&loadBalanceHosts=true" +
                "&loadBalanceBlacklistTimeout=5000" +
                "&loadBalanceStrategy=random" +
                "&useCompression=true" +
                "&cachePrepStmts=true" +
                "&prepStmtCacheSize=250" +
                "&prepStmtCacheSqlLimit=2048" +
                "&useServerPrepStmts=true" +
                "&useLegacyDatetimeCode=false" +
                "&zeroDateTimeBehavior=CONVERT_TO_NULL";

        DbConnectionInfo expected = getDbConnectionInfo();

        DbConnectionInfo result = parser.parse(url);
        assertEquals(expected, result);
    }

    private static DbConnectionInfo getDbConnectionInfo() {
        Map<String, String> expectedParams = new HashMap<>();

        expectedParams.put("user", "myuser");
        expectedParams.put("password", "mypassword");
        expectedParams.put("useSSL", "true");
        expectedParams.put("requireSSL", "true");
        expectedParams.put("verifyServerCertificate", "false");
        expectedParams.put("autoReconnect", "true");
        expectedParams.put("failOverReadOnly", "false");
        expectedParams.put("allowPublicKeyRetrieval", "true");
        expectedParams.put("serverTimezone", "UTC");
        expectedParams.put("characterEncoding", "UTF-8");
        expectedParams.put("useUnicode", "true");
        expectedParams.put("connectTimeout", "5000");
        expectedParams.put("socketTimeout", "10000");
        expectedParams.put("rewriteBatchedStatements", "true");
        expectedParams.put("loadBalanceHosts", "true");
        expectedParams.put("loadBalanceBlacklistTimeout", "5000");
        expectedParams.put("loadBalanceStrategy", "random");
        expectedParams.put("useCompression", "true");
        expectedParams.put("cachePrepStmts", "true");
        expectedParams.put("prepStmtCacheSize", "250");
        expectedParams.put("prepStmtCacheSqlLimit", "2048");
        expectedParams.put("useServerPrepStmts", "true");
        expectedParams.put("useLegacyDatetimeCode", "false");
        expectedParams.put("zeroDateTimeBehavior", "CONVERT_TO_NULL");


        return new DbConnectionInfo(DbType.MYSQL,
                List.of(new HostInfo("host1", PORT), new HostInfo("host2", PORT), new HostInfo("host3", PORT)),
                "mydatabase", expectedParams);
    }
}
