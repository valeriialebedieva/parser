package org.example.dburlparser.model;

import java.util.List;
import java.util.Map;

public record DbConnectionInfo(DbType type, List<HostInfo> hosts, String database, Map<String, String> properties) {
}