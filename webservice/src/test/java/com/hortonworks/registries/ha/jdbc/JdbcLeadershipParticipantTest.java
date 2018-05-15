package com.hortonworks.registries.ha.jdbc;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Base test for specific JDBC implementations.
 * Sadly, we can only test it for MySQL, because using H2 with compatibility mode doesn't support the syntax we need for
 * PostgreSQL.
 * We should embed the database with the test, this causing too much complexity and a waste of time and resources when
 * building.
 * It has been tested against local instance of PostgreSQL to check the correctness of the solution and syntax, but will
 * not be tested on each build. So, if you modify the queries, make sure to test it properly.
 */
public abstract class JdbcLeadershipParticipantTest {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcLeadershipParticipantTest.class);

    private static final String TABLE_NAME = "lock_table";
    private static final int DEFAULT_TIMEOUT = 1000;

    abstract String getDbType();

    @Before
    public void clearDatabase() throws SQLException {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(getDataSourceUrl(getDbType()));

        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS "+TABLE_NAME);
        }
    }

    @Test
    public void singleParticipant() throws Exception {
        Map<String, Object> conf = createConf(getDbType());

        String participant = "only-me";

        JdbcLeadershipParticipant jdbcLeadershipParticipant = new JdbcLeadershipParticipant();
        jdbcLeadershipParticipant.init(conf, participant);

        jdbcLeadershipParticipant.participateForLeadership();

        Assert.assertTrue(jdbcLeadershipParticipant.isLeader());
    }

    @Test
    public void twoParticipants() throws Exception {
        Map<String, Object> conf = createConf(getDbType());

        String participant1 = "foo-1";
        JdbcLeadershipParticipant jdbcLeadershipParticipant1 = new JdbcLeadershipParticipant();
        jdbcLeadershipParticipant1.init(conf, participant1);

        String participant2 = "foo-2";
        JdbcLeadershipParticipant jdbcLeadershipParticipant2 = new JdbcLeadershipParticipant();
        jdbcLeadershipParticipant2.init(conf, participant2);

        jdbcLeadershipParticipant1.participateForLeadership(); // First to ask for leadership
        Assert.assertTrue(jdbcLeadershipParticipant1.isLeader());
        jdbcLeadershipParticipant2.participateForLeadership();
        Assert.assertTrue(jdbcLeadershipParticipant1.isLeader());
        Assert.assertFalse(jdbcLeadershipParticipant2.isLeader());

        jdbcLeadershipParticipant1.exitFromLeaderParticipation(); // Current leader exit
        Thread.sleep(DEFAULT_TIMEOUT); // Wait for reelection
        Assert.assertFalse(jdbcLeadershipParticipant1.isLeader());
        Assert.assertTrue(jdbcLeadershipParticipant2.isLeader());
    }

    @Test
    public void twoParticipantsReparticipate() throws Exception {
        Map<String, Object> conf = createConf(getDbType());

        String participant1 = "foo-1";
        JdbcLeadershipParticipant jdbcLeadershipParticipant1 = new JdbcLeadershipParticipant();
        jdbcLeadershipParticipant1.init(conf, participant1);

        String participant2 = "foo-2";
        JdbcLeadershipParticipant jdbcLeadershipParticipant2 = new JdbcLeadershipParticipant();
        jdbcLeadershipParticipant2.init(conf, participant2);

        jdbcLeadershipParticipant1.participateForLeadership(); // First to ask for leadership
        Assert.assertTrue(jdbcLeadershipParticipant1.isLeader());
        jdbcLeadershipParticipant2.participateForLeadership();
        Assert.assertTrue(jdbcLeadershipParticipant1.isLeader());
        Assert.assertFalse(jdbcLeadershipParticipant2.isLeader());

        jdbcLeadershipParticipant1.exitFromLeaderParticipation();
        Thread.sleep(DEFAULT_TIMEOUT); // Wait for reelection
        jdbcLeadershipParticipant1.participateForLeadership();
        Assert.assertFalse(jdbcLeadershipParticipant1.isLeader());
        Assert.assertTrue(jdbcLeadershipParticipant2.isLeader());
    }

    @Test
    public void twoParticipantsReparticipateWithoutWait() throws Exception {
        Map<String, Object> conf = createConf(getDbType(), 42000); // Huge timeout to avoid automatic reelection

        String participant1 = "foo-1";
        JdbcLeadershipParticipant jdbcLeadershipParticipant1 = new JdbcLeadershipParticipant();
        jdbcLeadershipParticipant1.init(conf, participant1);

        String participant2 = "foo-2";
        JdbcLeadershipParticipant jdbcLeadershipParticipant2 = new JdbcLeadershipParticipant();
        jdbcLeadershipParticipant2.init(conf, participant2);

        jdbcLeadershipParticipant1.participateForLeadership(); // First to ask for leadership
        Assert.assertTrue(jdbcLeadershipParticipant1.isLeader());
        jdbcLeadershipParticipant2.participateForLeadership();
        Assert.assertTrue(jdbcLeadershipParticipant1.isLeader());
        Assert.assertFalse(jdbcLeadershipParticipant2.isLeader());

        jdbcLeadershipParticipant1.exitFromLeaderParticipation();
        jdbcLeadershipParticipant1.participateForLeadership();
        Assert.assertTrue(jdbcLeadershipParticipant1.isLeader()); // The first one to ask for who is the leader will get it
        Assert.assertFalse(jdbcLeadershipParticipant2.isLeader());
    }

    private Map<String, Object> createConf(String dbType) {
        return createConf(dbType, DEFAULT_TIMEOUT);
    }

    private Map<String, Object> createConf(String dbType, int timeout) {
        Map<String, Object> dbProperties = new HashMap<>();
        dbProperties.put("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
        dbProperties.put("dataSource.url", getDataSourceUrl(dbType));
        Map<String, Object> conf = new HashMap<>();
        conf.put("table", TABLE_NAME);
        conf.put("timeout", timeout);
        conf.put("db.properties", dbProperties);
        conf.put("db.type", dbType);
        return conf;
    }

    String getDataSourceUrl(String dbType) {
        return "jdbc:h2:mem:test;MODE="+dbType+";DATABASE_TO_UPPER=false;DB_CLOSE_ON_EXIT=FALSE";
    }

}
