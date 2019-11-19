package com.sd.connector;

import com.sd.model.OutData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Author: Will Fan
 * Created: 2019/10/24 9:45
 * Description:
 */
public class PgsqlWriter2 extends RichSinkFunction<OutData> {
    private Calendar calendar;
    private Connection connection;
    private PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
        calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));

        Class.forName("org.postgresql.Driver");
        connection =
                DriverManager.getConnection(CommonDefs.PGSQL_HOST, CommonDefs.PGSQL_USER, CommonDefs.PGSQL_PWD);

        SimpleDateFormat sdf = new SimpleDateFormat("yyMM");
        String dateStr = sdf.format(new java.util.Date());
        String tblName = "iot_stat_kafka_" + dateStr;
        createTable(connection, tblName);
        statement = connection.prepareStatement(getInsertSql(tblName));

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
    public void invoke(OutData value, Context context) throws Exception {
        calendar.setTimeInMillis(value.f4);
        if (calendar.get(Calendar.DAY_OF_MONTH) == 1) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyMM");
            String dateStr = sdf.format(calendar.getTime());
            String tblName = "iot_stat_kafka_" + dateStr;
            createTable(connection, tblName);
            this.statement = this.connection.prepareStatement(getInsertSql(tblName));
        }

        statement.setString(1, value.f0);
        statement.setString(2, value.f1);
        statement.setString(3, value.f2);
        statement.setDate(4, new Date(value.f4));
        statement.setLong(5, value.f5);
        statement.setLong(6, value.f5);
        statement.addBatch();
        statement.executeBatch();
    }

    private void createTable(Connection connection, String tblNameStr) {
        final String sql =
                "CREATE TABLE " + tblNameStr +
                "(devid varchar NOT NULL" +
                ", productkey varchar NOT NULL" +
                ", date date NOT NULL" +
                ", count int4 NOT NULL" +
                ", msgtype varchar NOT NULL" +
//                ", PRIMARY KEY (devid, msgtype, date)" +
                ")";

        Statement statement = null;
        try {
            statement = connection.createStatement();

            statement.execute(sql);
        } catch (Exception se) {
            se.printStackTrace();
        }
    }

    private String getInsertSql(String tblName) {
        return "INSERT INTO " + tblName
                + " (devid,productkey, msgtype, date, count)"
                + " VALUES (?, ?, ?, ?, ?)"
//                + " ON CONFLICT (devid, msgtype, date) DO UPDATE SET"
//                + " count=?"
                ;
    }
}
