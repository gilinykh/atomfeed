package org.ict4h.atomfeed.server.repository.jdbc;

import org.ict4h.atomfeed.Configuration;
import org.ict4h.atomfeed.jdbc.ConnectionPool;
import org.ict4h.atomfeed.jdbc.JdbcResultSetMapper;
import org.ict4h.atomfeed.jdbc.JdbcUtils;
import org.ict4h.atomfeed.server.domain.EventRecordQueueItem;
import org.ict4h.atomfeed.server.exceptions.AtomFeedRuntimeException;
import org.ict4h.atomfeed.server.repository.AllEventRecordsQueue;

import java.sql.*;
import java.util.List;

public class JdbcPooledAllEventRecordsQueue implements AllEventRecordsQueue {

    private static final String FIELD_LIST = "id, uuid, title, timestamp, uri, object, category, tags";
    private ConnectionPool connectionPool;

    public JdbcPooledAllEventRecordsQueue(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    @Override
    public void add(EventRecordQueueItem eventRecord) {
        Connection connection = connectionPool.lease();
        PreparedStatement stmt = null;
        try {
            String insertSql = String.format("insert into %s (uuid, title, uri, object,category, timestamp, tags) values (?, ?, ?, ?, ?, ?, ?)",
                    JdbcUtils.getTableName(Configuration.getInstance().getSchema(), "event_records_queue"));
            stmt = connection.prepareStatement(insertSql);
            stmt.setString(1, eventRecord.getUuid());
            stmt.setString(2, eventRecord.getTitle());
            stmt.setString(3, eventRecord.getUri());
            stmt.setString(4, eventRecord.getContents());
            stmt.setString(5, eventRecord.getCategory());
            stmt.setTimestamp(6, new Timestamp(eventRecord.getTimeStamp().getTime()));
            stmt.setString(7, eventRecord.getTags());
            stmt.executeUpdate();
        } catch (SQLException e) {
            connectionPool.release(connection);
            throw new AtomFeedRuntimeException(e);
        } finally {
            close(stmt);
            connectionPool.release(connection);
        }
    }

    @Override
    public EventRecordQueueItem get(String uuid) {
        Connection connection = connectionPool.lease();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            String sql = String.format("select " + FIELD_LIST + " from %s where uuid = ?",
                    JdbcUtils.getTableName(Configuration.getInstance().getSchema(), "event_records_queue"));
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, uuid);
            rs = stmt.executeQuery();
            List<EventRecordQueueItem> events = mapEventRecords(rs);
            if ((events != null) && !events.isEmpty()) {
                return events.get(0);
            }
        } catch (SQLException e) {
            connectionPool.release(connection);
            throw new AtomFeedRuntimeException(e);
        } finally {
            closeAll(stmt, rs);
            connectionPool.release(connection);
        }
        return null;
    }

    private void closeAll(PreparedStatement stmt, ResultSet rs) {
        close(rs);
        close(stmt);
    }

    private void close(AutoCloseable rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            throw new AtomFeedRuntimeException(e);
        }
    }

    @Override
    public List<EventRecordQueueItem> getAll() {
        Connection connection = connectionPool.lease();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            String sql = String.format("select " + FIELD_LIST + " from %s",
                    JdbcUtils.getTableName(Configuration.getInstance().getSchema(), "event_records_queue"));
            stmt = connection.prepareStatement(sql);
            rs = stmt.executeQuery();
            return mapEventRecords(rs);
        } catch (SQLException e) {
            connectionPool.release(connection);
            throw new AtomFeedRuntimeException(e);
        } finally {
            closeAll(stmt, rs);
            connectionPool.release(connection);
        }
    }

    @Override
    public void delete(String uuid) {
        Connection connection = connectionPool.lease();
        PreparedStatement stmt = null;
        try {
            String sql = String.format("delete from %s where uuid = ?",
                    JdbcUtils.getTableName(Configuration.getInstance().getSchema(), "event_records_queue"));
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, uuid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            connectionPool.release(connection);
            throw new AtomFeedRuntimeException(e);
        } finally {
            close(stmt);
            connectionPool.release(connection);
        }
    }

    private List<EventRecordQueueItem> mapEventRecords(ResultSet results) {
        return new JdbcResultSetMapper<EventRecordQueueItem>().mapResultSetToObject(results, EventRecordQueueItem.class);
    }
}