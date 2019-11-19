package com.sd.connector;

import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Author: Will Fan
 * Created: 2019/10/11 10:38
 * Description:
 */
public class Main {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        Jedis jedis = new Jedis(CommonDefs.REDIS_HOST);
////        jedis.hset("")
//        jedis.select(2);
//        jedis.hset("test:123", "name", "teset");
//        jedis.hset("test:123", "age", "234");
//        jedis.close();

        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(1572571229000L);
        int hour = c.get(Calendar.HOUR);
        int minute = c.get(Calendar.MINUTE);
        int day = c.get(Calendar.DAY_OF_MONTH);
        System.out.println(day + ":" + hour + ":" + minute);
        SimpleDateFormat sdf = new SimpleDateFormat("yyMM");

        String dateStr = sdf.format(c.getTime());


        Class.forName("org.postgresql.Driver");
        Connection connection =
                DriverManager.getConnection(CommonDefs.PGSQL_HOST, CommonDefs.PGSQL_USER, CommonDefs.PGSQL_PWD);
//        String dateStr = new SimpleDateFormat("yyMM").format(new Date());
        String tblNameStr = "\"public\".\"iot_stat_kafka_"+dateStr+"\"";

        if (day == 1)
            createTable(connection, tblNameStr);
    }

    private static void createTable(Connection connection, String tblNameStr) throws SQLException {
        Statement statement = connection.createStatement();

        final String sql =
                "DROP TABLE IF EXISTS " + tblNameStr + ";\n" +
                        "CREATE TABLE " +tblNameStr +
                        " (\n" +
                        "  \"devid\" varchar COLLATE \"pg_catalog\".\"default\" NOT NULL,\n" +
                        "  \"productkey\" varchar COLLATE \"pg_catalog\".\"default\" NOT NULL,\n" +
                        "  \"date\" date NOT NULL,\n" +
                        "  \"count\" int4 NOT NULL,\n" +
                        "  \"msgtype\" varchar COLLATE \"pg_catalog\".\"default\" NOT NULL,\n" +
                        "  PRIMARY KEY (\"devid\", \"msgtype\", \"date\")\n" +
                        ")";

        statement.execute(sql);
    }
}
