package com.example.jobs.domain;

import java.sql.ResultSet;
import java.sql.SQLException;

public record OutboxEvent(long id, String exchange, String routingKey, String payload) {
    public static OutboxEvent map(ResultSet rs, int rowNum) throws SQLException {
        return new OutboxEvent(rs.getLong("id"), rs.getString("exchange"),
                rs.getString("routing_key"), rs.getString("payload"));
    }
}
