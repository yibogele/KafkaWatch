package com.fan.connector;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Author: Will Fan
 * Created: 2019/10/11 11:17
 * Description:
 */
public class PgsqlWriter extends RichSinkFunction<Tuple6<String, String, String, Long, Long, Long>> {
    private static final String UPSERT_STAT =
            "INSERT INTO iot_stat_kafka (devid,productkey, msgtype, date, count) "
                    + "VALUES (?, ?, ?, ?, ?) "
                    + "ON CONFLICT (devid, msgtype, date) DO UPDATE SET "
                    + "count=?";

    private Connection connection;
    private PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);

        Class.forName("org.postgresql.Driver");
        connection =
                DriverManager.getConnection(CommonDefs.PGSQL_HOST, CommonDefs.PGSQL_USER, CommonDefs.PGSQL_PWD);

        statement = connection.prepareStatement(UPSERT_STAT);

    }

    @Override
    public void close() throws Exception {
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }

        super.close();
    }

    @Override
    public void invoke(Tuple6<String, String, String, Long, Long, Long> value, Context context) throws Exception {
        statement.setString(1, value.f0);
        statement.setString(2, value.f1);
        statement.setString(3, value.f2);
        statement.setDate(4, new Date(value.f4));
        statement.setLong(5, value.f5);
        statement.setLong(6, value.f5);
        statement.addBatch();
        statement.executeBatch();
    }
}
