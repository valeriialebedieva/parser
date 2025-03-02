package org.example.dburlparser.model;

public enum DbType {
    MYSQL("jdbc:mysql://"),
    CASSANDRA("jdbc:cassandra://"),
    ORACLE("jdbc:oracle:thin:@"),
    UNKNOWN("");

    private final String prefix;

    DbType(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    public static DbType getDbType(String url) {
        for (DbType type : values()) {
            if (url.startsWith(type.prefix)) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
