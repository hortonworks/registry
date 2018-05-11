package com.hortonworks.registries.ha.jdbc;

public class MysqlLeadershipParticipantTest extends JdbcLeadershipParticipantTest {
    @Override
    String getDbType() {
        return "MySQL";
    }
}
