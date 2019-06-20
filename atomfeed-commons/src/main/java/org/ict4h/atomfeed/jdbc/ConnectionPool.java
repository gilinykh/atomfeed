package org.ict4h.atomfeed.jdbc;

import java.sql.Connection;

public interface ConnectionPool {

    Connection lease();

    void release(Connection connection);
}
