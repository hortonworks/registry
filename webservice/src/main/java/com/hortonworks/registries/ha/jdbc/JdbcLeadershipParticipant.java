package com.hortonworks.registries.ha.jdbc;

import com.google.common.base.Preconditions;
import com.hortonworks.registries.common.ha.LeadershipParticipant;
import com.hortonworks.registries.storage.impl.jdbc.provider.QueryExecutorFactory;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * {@link LeadershipParticipant} implementation for databases accessible through JDBC (like MySQL or PostgreSQL).
 *
 * It uses a table to manage the leader election. Taking advantage of primary key collision, we can handle only one
 * leader at the same time.
 * Then, using upserts, we can handle to update the timeout if we are the leader, or taking leadership if already timed out.
 *
 * Unfortunately, there is no standard SQL statement implemented in major vendors to handle upserts properly, so we need
 * to handle this logic separatelly across different DB types. Right now only MySQL and PostgreSQL implementations are
 * provided. Oracle is not provided as it is more complex to set up a properly environment to test it (Licenses and stuff)
 *
 * To check who owns the lock of release it, is as simple as a select query to retrieve the row and delete the row itself.
 *
 * When deleting the row, the next one who tries to achieve leadership will succeed.
 *
 * This approach is preferred instead of using user-defined locks (i.e. GET_LOCK from MySQL or pg_advisory_lock from
 * PostgreSQL, because:
 * - We must allocate a dedicated connection that lives for the duration of the lock (until the application stops)
 * - This doesn't play too well with  typical connection pools
 * - There is no visibility into "who" is holding the lock. So, we would still to store the server url of the leader
 *   somewhere else, ending up with a similar complexity solution.
 * - If the application hangs (but does not die or close the connection), the lock is still being held. There is no
 *   "keepalive" or "timeout" requirement on the lock's side.
 *
 */
public class JdbcLeadershipParticipant implements LeadershipParticipant {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcLeadershipParticipant.class);

    public static final String DB_TYPE_CONFIG = "db.type";
    public static final String DB_PROPERTIES_CONFIG = "db.properties";
    public static final String TABLE_NAME_CONFIG = "table";
    public static final String LOCK_ID_CONFIG = "lock";
    public static final String TIMEOUT_CONFIG = "timeout";
    public static final String REFRESH_CONFIG = "refresh";

    public static final int DEFAULT_LOCK_ID = 1;
    public static final long DEFAULT_TIMEOUT = 5000;

    private static final int MAX_RETRIES = 5;

    private static final String CREATE_STATEMENT =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    "  lock_id INTEGER NOT NULL," +
                    "  server_url VARCHAR(256) NOT NULL," +
                    "  last_seen_active TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "  PRIMARY KEY (lock_id)" +
                    ")";

    private static final String LOCK_STATEMENT_MYSQL =
            "INSERT INTO %s (lock_id, server_url, last_seen_active) VALUES (?, ?, CURRENT_TIMESTAMP) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "    server_url = CASE WHEN last_seen_active < DATEADD('MILLISECOND', ?, CURRENT_TIMESTAMP) THEN VALUES(server_url) ELSE server_url END," +
                    "    last_seen_active = CASE WHEN server_url = VALUES(server_url) THEN VALUES(last_seen_active) ELSE last_seen_active END";

    private static final String LOCK_STATEMENT_POSTGRESQL =
            "INSERT INTO %s AS ORIGINAL (lock_id, server_url, last_seen_active) VALUES (?, ?, CURRENT_TIMESTAMP) " +
                    "ON CONFLICT (lock_id) DO UPDATE SET " +
                    "    server_url = CASE WHEN ORIGINAL.last_seen_active < CURRENT_TIMESTAMP + ? * INTERVAL '1 milliseconds' THEN EXCLUDED.server_url ELSE ORIGINAL.server_url END," +
                    "    last_seen_active = CASE WHEN ORIGINAL.server_url = EXCLUDED.server_url THEN EXCLUDED.last_seen_active ELSE ORIGINAL.last_seen_active END";

    private static final String GET_STATEMENT =
            "SELECT server_url AS leader FROM %s WHERE lock_id = ?";

    private static final String UNLOCK_STATEMENT =
            "DELETE FROM %s WHERE lock_id = ?";

    private Timer executor;

    private String tableName;
    private int lock;
    private int timeout;
    private int refresh;

    private String dbType;
    private QueryExecutor queryExecutor;
    private String serverUrl;

    @Override
    public void init(Map<String, Object> config, String participantId) {
        Preconditions.checkNotNull(participantId, "participantId can not be null");
        Preconditions.checkNotNull(config, "conf can not be null");
        if(!config.containsKey(DB_TYPE_CONFIG)) {
            throw new IllegalArgumentException("db.type should be set on jdbc properties");
        }

        LOG.info("Received configuration : [{}]", config);

        this.serverUrl = participantId;

        this.dbType = ((String) config.get(DB_TYPE_CONFIG)).toLowerCase();
        Map<String, Object> dbProperties = (Map<String, Object>) config.get(DB_PROPERTIES_CONFIG);
        this.queryExecutor = QueryExecutorFactory.get(dbType, dbProperties);

        this.tableName = (String) config.get(TABLE_NAME_CONFIG);
        this.lock = (Integer) config.getOrDefault(LOCK_ID_CONFIG, DEFAULT_LOCK_ID);
        this.timeout = (Integer) config.getOrDefault(TIMEOUT_CONFIG, DEFAULT_TIMEOUT);
        this.refresh = (Integer) config.getOrDefault(REFRESH_CONFIG, this.timeout/4);

        initDB();
    }

    private void initDB() {
        try (Connection connection = queryExecutor.getConnection();
             Statement statement = connection.createStatement())
        {
            statement.executeUpdate(String.format(CREATE_STATEMENT, tableName));
        } catch (SQLException e) {
            LOG.error("Cannot create lock table", e);
        }
    }

    @Override
    public void participateForLeadership() throws Exception{
        // Blockingly participate for leadership the first time
        updateParticipationForLeadership();

        // Schedule next updates
        this.executor = new Timer();
        this.executor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    updateParticipationForLeadership();
                } catch (SQLException e) {
                    LOG.error("Cannot update participation for leadership", e);
                }
            }
        }, refresh, refresh);
    }

    private void updateParticipationForLeadership() throws SQLException {
        try (Connection connection = queryExecutor.getConnection();
             PreparedStatement statement = connection.prepareStatement(String.format(getLockStatement(), tableName)))
        {
            statement.setInt(1, lock);
            statement.setString(2, serverUrl);
            statement.setInt(3, -timeout);

            statement.executeUpdate();
        }
    }

    private String getLockStatement() {
        switch (dbType) {
            case "mysql": return LOCK_STATEMENT_MYSQL;
            case "postgresql": return LOCK_STATEMENT_POSTGRESQL;
            default: throw new IllegalArgumentException("Unsupported storage provider type: " + dbType);
        }
    }

    @Override
    public String getCurrentLeader() throws Exception {
        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            try (Connection connection = queryExecutor.getConnection();
                 PreparedStatement statement = connection.prepareStatement(String.format(GET_STATEMENT, tableName)))
            {
                statement.setInt(1, lock);

                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    return resultSet.getString(1);
                } else {
                    // This may happen if the leader is gone, and nobody tried to get leadership yet. So we will try it and check it again
                    updateParticipationForLeadership();
                }
            }
        }
        throw new IllegalStateException("Could not get current leader");
    }

    @Override
    public boolean isLeader() {
        try {
            String currentLeader = getCurrentLeader();
            return currentLeader.equals(this.serverUrl);
        } catch (Exception e) {
            LOG.warn("Could not deterime if leader, assuming not", e);
            return false;
        }
    }

    @Override
    public void exitFromLeaderParticipation() throws Exception {
        this.executor.cancel();
        this.executor = null;
        if (isLeader()) {
            try (Connection connection = queryExecutor.getConnection();
                 PreparedStatement statement = connection.prepareStatement(String.format(UNLOCK_STATEMENT, tableName)))
            {
                statement.setInt(1, lock);
                statement.executeUpdate();
            }
        }
    }

    @Override
    public void close() {
        if (this.executor != null) {
            executor.cancel();
            executor = null;
        }
        this.queryExecutor.cleanup();
    }

}
