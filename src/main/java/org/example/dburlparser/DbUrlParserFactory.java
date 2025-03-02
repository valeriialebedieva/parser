package org.example.dburlparser;

import org.example.dburlparser.model.DbType;
import org.example.dburlparser.parser.impl.nosql.CassandraUrlParser;
import org.example.dburlparser.parser.DbUrlParser;
import org.example.dburlparser.parser.impl.sql.BaseUrlParser;
import org.example.dburlparser.parser.impl.sql.OracleUrlParser;

public class DbUrlParserFactory {
    public static DbUrlParser getParser(String url) {
        return switch (DbType.getDbType(url)) {
            case MYSQL -> new BaseUrlParser();
            case CASSANDRA -> new CassandraUrlParser();
            case ORACLE -> new OracleUrlParser();
            default -> throw new IllegalArgumentException("Unsupported database type.");
        };
    }
}
