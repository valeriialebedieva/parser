package org.example.dburlparser;

import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.parser.DbUrlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        String mysqlUrl = "jdbc:mysql://db1.example.com:3306,db2.example.com:3307/mydb?useSSL=true&serverTimezone=UTC&autoReconnect=true";
        String oracleUrl = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=myservice)))?user=myuser&password=mypassword&ReadTimeout=30000&V8Compatible=true&oracle.net.CONNECT_TIMEOUT=5000&TcpNoDelay=true&useFetchSizeWithLongColumn=true&autoCommitSpecCompliant=false&defaultRowPrefetch=100";
        String cassandraUrl = "jdbc:cassandra://node1.example.com,node2.example.com:9043/keyspace1?consistency=QUORUM&loadBalancing=RoundRobin";

        parse(mysqlUrl);
        parse(oracleUrl);
        parse(cassandraUrl);
    }

    private static void parse(String url) {
        try {
            DbUrlParser parser = DbUrlParserFactory.getParser(url);
            DbConnectionInfo connectionInfo = parser.parse(url);
            System.out.println(connectionInfo);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            LOGGER.error("Failed to parse URL", e);
        }
    }
}
