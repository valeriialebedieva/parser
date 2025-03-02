package org.example.dburlparser.parser;

import org.example.dburlparser.model.DbConnectionInfo;
import org.example.dburlparser.model.HostInfo;

import java.util.*;

public interface DbUrlParser {
    DbConnectionInfo parse(String url);

    default List<HostInfo> parseHosts(String hostsWithPorts, int defaultPort) {
        List<HostInfo> hosts = new ArrayList<>();
        for (String hostPort : hostsWithPorts.split(",")) {
            String[] hostParts = hostPort.split(":");
            if (hostParts.length == 0 || hostParts[0].isEmpty()) {
                throw new IllegalArgumentException("Invalid host in URL: " + hostsWithPorts);
            }
            String host = hostParts[0];
            int port = (hostParts.length > 1) ? Integer.parseInt(hostParts[1]) : defaultPort;
            hosts.add(new HostInfo(host, port));
        }
        return hosts;
    }

    default Map<String, String> parseParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query != null && !query.isEmpty()) {
            for (String pair : query.split("&")) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    params.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return params;
    }

    default String extractMainPart(String url) {
        return url.split("\\?", 2)[0];
    }

    default Map<String, String> extractQueryParams(String url) {
        String[] urlParts = url.split("\\?", 2);
        return (urlParts.length > 1) ? parseParams(urlParts[1]) : Collections.emptyMap();
    }

    default String extractHostsPart(String mainPart) {
        return mainPart.contains("/") ? mainPart.substring(0, mainPart.indexOf("/")) : mainPart;
    }

    default String extractDatabaseName(String mainPart) {
        return mainPart.contains("/") ? mainPart.substring(mainPart.indexOf("/") + 1) : "";
    }
}
