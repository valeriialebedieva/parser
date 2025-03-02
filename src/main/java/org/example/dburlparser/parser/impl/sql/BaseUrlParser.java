package org.example.dburlparser.parser.impl.sql;

import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.model.DbType;
import org.example.dburlparser.model.HostInfo;
import org.example.dburlparser.parser.DbUrlParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BaseUrlParser implements DbUrlParser {
    private static final Logger logger = LoggerFactory.getLogger(BaseUrlParser.class);
    private static final int DEFAULT_PORT = 3306;
    private static final String ERROR_MESSAGE = "Invalid MySQL URL: Missing database name";

    @Override
    public DbConnectionInfo parse(String url) {
        logger.info("Parsing MySQL DB URL");
        String prefix = DbType.MYSQL.getPrefix();
        String rawUrl = url.substring(prefix.length()).trim();

        String mainPart = extractMainPart(rawUrl);
        Map<String, String> properties = extractQueryParams(rawUrl);

        if (!mainPart.contains("/")) {
            logger.error(ERROR_MESSAGE);
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }

        String hostsPart = extractHostsPart(mainPart);
        String database = extractDatabaseName(mainPart);

        List<HostInfo> hosts = parseHosts(hostsPart, DEFAULT_PORT);
        return new DbConnectionInfo(DbType.MYSQL, hosts, database, properties);
    }
}
