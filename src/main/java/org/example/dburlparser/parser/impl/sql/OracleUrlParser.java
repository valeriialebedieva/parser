package org.example.dburlparser.parser.impl.sql;

import org.example.dburlparser.model.DbType;
import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.model.HostInfo;
import org.example.dburlparser.parser.DbUrlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OracleUrlParser implements DbUrlParser {
    private static final Logger logger = LoggerFactory.getLogger(OracleUrlParser.class);
    private static final int DEFAULT_PORT = 1521;
    private static final String ERROR_MESSAGE = "Invalid Oracle Service Name URL";
    private static final String INVALID_ORACLE_SID_URL = "Invalid Oracle SID URL";
    private static final String MISSING_SERVICE_NAME_OR_SID = "Invalid Oracle TNS URL: Missing SERVICE_NAME or SID";

    @Override
    public DbConnectionInfo parse(String url) {
        logger.info("Parsing Oracle DB URL");
        String prefix = DbType.ORACLE.getPrefix();
        String rawUrl = url.substring(prefix.length()).trim();

        if (rawUrl.startsWith("(")) {
            return parseTnsFormat(rawUrl);
        } else if (rawUrl.startsWith("//")) {
            return parseServiceNameFormat(rawUrl);
        } else {
            return parseSidFormat(rawUrl);
        }
    }

    private DbConnectionInfo parseSidFormat(String url) {
        logger.debug("Parsing SID format");
        String[] urlParts = url.split("\\?", 2);
        String mainPart = urlParts[0];
        Map<String, String> properties = (urlParts.length > 1) ? parseParams(urlParts[1]) : new HashMap<>();

        String[] parts = mainPart.split(":");
        if (parts.length < 3) {
            logger.error(INVALID_ORACLE_SID_URL);
            throw new IllegalArgumentException(INVALID_ORACLE_SID_URL);
        }

        String hostsWithPorts = parts[0];
        String database = parts[2];

        List<HostInfo> hosts = parseHosts(hostsWithPorts, DEFAULT_PORT);
        return new DbConnectionInfo(DbType.ORACLE, hosts, database, properties);
    }

    private DbConnectionInfo parseServiceNameFormat(String url) {
        logger.debug("Parsing Service Name format");
        String[] urlParts = url.split("\\?", 2);
        String mainPart = urlParts[0];
        Map<String, String> properties = (urlParts.length > 1) ? parseParams(urlParts[1]) : new HashMap<>();

        String[] parts = mainPart.split("/");
        if (parts.length < 4) {
            logger.error(ERROR_MESSAGE);
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }

        String hostsWithPorts = parts[2];
        String database = parts[3];

        List<HostInfo> hosts = parseHosts(hostsWithPorts, DEFAULT_PORT);
        return new DbConnectionInfo(DbType.ORACLE, hosts, database, properties);
    }

    private DbConnectionInfo parseTnsFormat(String url) {
        logger.debug("Parsing TNS format");
        List<HostInfo> hosts = new ArrayList<>();
        String database = "";
        Map<String, String> properties = new HashMap<>();

        int queryIndex = url.indexOf('?');
        if (queryIndex != -1) {
            properties = parseParams(url.substring(queryIndex + 1));
            url = url.substring(0, queryIndex);
            logger.debug("Extracted query parameters");
        }

        int startIndex = 0;
        while ((startIndex = url.indexOf("(HOST=", startIndex)) != -1) {
            try {
                int endIndex = url.indexOf(")", startIndex);
                String host = url.substring(startIndex + 6, endIndex).trim();

                startIndex = url.indexOf("(PORT=", endIndex);
                int port = DEFAULT_PORT;
                if (startIndex != -1) {
                    endIndex = url.indexOf(")", startIndex);
                    port = Integer.parseInt(url.substring(startIndex + 6, endIndex).trim());
                }

                hosts.add(new HostInfo(host, port));
                startIndex = endIndex;
            } catch (Exception e) {
                logger.error("Error parsing TNS host/port", e);
                throw new IllegalArgumentException("Invalid Oracle TNS URL", e);
            }
        }

        if (url.contains("(SERVICE_NAME=")) {
            database = extractValue(url, "(SERVICE_NAME=");
        } else if (url.contains("(SID=")) {
            database = extractValue(url, "(SID=");
        } else {
            logger.error(MISSING_SERVICE_NAME_OR_SID);
            throw new IllegalArgumentException(MISSING_SERVICE_NAME_OR_SID);
        }
        return new DbConnectionInfo(DbType.ORACLE, hosts, database, properties);
    }

    private String extractValue(String url, String key) {
        int startIndex = url.indexOf(key);
        if (startIndex == -1) return "";
        int endIndex = url.indexOf(")", startIndex);
        return url.substring(startIndex + key.length(), endIndex).trim();
    }
}
