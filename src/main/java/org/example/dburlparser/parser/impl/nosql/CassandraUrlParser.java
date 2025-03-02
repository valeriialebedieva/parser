package org.example.dburlparser.parser.impl.nosql;

import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.model.DbType;
import org.example.dburlparser.model.HostInfo;
import org.example.dburlparser.parser.DbUrlParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraUrlParser implements DbUrlParser {
    private static final Logger logger = LoggerFactory.getLogger(CassandraUrlParser.class);
    private static final int DEFAULT_PORT = 9042;
    private static final String ERROR_MESSAGE = "Invalid Cassandra URL: Missing keyspace";

    @Override
    public DbConnectionInfo parse(String url) {
        logger.info("Parsing Cassandra DB URL");
        String prefix = DbType.CASSANDRA.getPrefix();
        String rawUrl = url.substring(prefix.length()).trim();

        // Removed user credentials (if exists)
        int atIndex = rawUrl.indexOf('@');
        if (atIndex != -1) {
            rawUrl = rawUrl.substring(atIndex + 1);
        }

        String[] urlParts = rawUrl.split("\\?", 2);
        String mainPart = urlParts[0];
        Map<String, String> properties = (urlParts.length > 1) ? parseParams(urlParts[1]) : Collections.emptyMap();

        String[] mainParts = mainPart.split("/", 2);
        if (mainParts.length < 2 || mainParts[1].isEmpty()) {
            logger.error(ERROR_MESSAGE);
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }

        String hostsPart = mainParts[0];
        String keyspace = mainParts[1];

        List<HostInfo> hosts = parseHosts(hostsPart, DEFAULT_PORT);

        return new DbConnectionInfo(DbType.CASSANDRA, hosts, keyspace, properties);
    }
}
