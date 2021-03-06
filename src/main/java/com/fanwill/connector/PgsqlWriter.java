package com.fanwill.connector;

import com.fanwill.model.OutData;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Date;
import java.sql.*;
import java.util.*;

/**
 * Author: Will Fan
 * Created: 2019/10/11 11:17
 * Description:
 */
public class PgsqlWriter extends RichSinkFunction<OutData> implements CheckpointListener, CheckpointedFunction {
    private static final String UPSERT_STAT =
            "INSERT INTO kafka_iot_stat (devid,productkey, msgtype, date, count, update ) "
                    + "VALUES (?, ?, ?, ?, ?, ?) "
//            + "ON CONFLICT (devid, msgtype, date) DO UPDATE SET "
//            + "count=?"
            ;

    private BasicDataSource connectionPool;
//    private Connection connection;
    //    private PreparedStatement statement;
    private ParameterTool gConf;

    //
    private List<OutData> pendingUpserts = new ArrayList<>();
    private Map<Long, List<OutData>> pendingUpsertsPerCheckpoint = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
        gConf = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        initPool();

//        connection = connectionPool.getConnection();
//        statement = connection.prepareStatement(UPSERT_STAT);
    }

    @Override
    public void close() throws Exception {
        if (connectionPool != null) {
            connectionPool.close();
        }

        super.close();
    }

    @Override
    public void invoke(OutData value, Context context) throws Exception {
        pendingUpserts.add(value);
    }

    private void initPool() {
        connectionPool = new BasicDataSource();
        connectionPool.setDriverClassName("org.postgresql.Driver");
        connectionPool.setUrl(gConf.get("pgsql.host", CommonDefs.PGSQL_HOST));
        connectionPool.setUsername(gConf.get("pgsql.user", CommonDefs.PGSQL_USER));
        connectionPool.setPassword(gConf.get("pgsql.pwd", CommonDefs.PGSQL_PWD));
//        connectionPool.setPoolPreparedStatements(true);
        connectionPool.setInitialSize(5);

//        System.out.println("/////////////////////////");
//        System.out.println(gConf.get("pgsql.host", CommonDefs.PGSQL_HOST));
    }


    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("<<<notifyCheckpointComplete>>>");

        if (pendingUpsertsPerCheckpoint.isEmpty())
            return;

        Iterator<Map.Entry<Long, List<OutData>>> pendingCheckpointsIt =
                pendingUpsertsPerCheckpoint.entrySet().iterator();
        Connection connection = connectionPool.getConnection();
        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(UPSERT_STAT);
            int batchLimit = 1000;
            try {
                while (pendingCheckpointsIt.hasNext()) {
                    Map.Entry<Long, List<OutData>> entry = pendingCheckpointsIt.next();
                    Long pastCheckpointId = entry.getKey();

                    List<OutData> pendingOuts = entry.getValue();

                    if (pastCheckpointId <= checkpointId) {
                        for (OutData outData : pendingOuts) {
                            preparedStatement.setString(1, outData.f0);
                            preparedStatement.setString(2, outData.f1);
                            preparedStatement.setString(3, outData.f2);

                            preparedStatement.setDate(4, new Date(outData.f4 + 8 * 60 * 60 * 1000));
                            preparedStatement.setLong(5, outData.f5);
                            preparedStatement.setTimestamp(6, new Timestamp(System.currentTimeMillis() + 8 * 60 * 60 * 1000));
//                    System.out.println("["+outData.f4+"========="+System.currentTimeMillis()+"]");
                            preparedStatement.addBatch();

                            batchLimit--;
                            if (batchLimit == 0) {
                                System.out.println("//////////////////////////////executeBatch///////////////////////");
                                preparedStatement.executeBatch();
                                preparedStatement.clearBatch();
                                batchLimit = 1000;
                            }
                        }

                        pendingCheckpointsIt.remove();
                    }

                }
            } catch (SQLException se) {
                System.out.println("Exception when insert into PostgresSql:" + se);
            } finally {
                preparedStatement.executeBatch();
                connection.commit();
                if (preparedStatement != null)
                    preparedStatement.close();
            }
        } finally {
            connection.setAutoCommit(autoCommit);
            connection.close();
        }

//        statement.close();
//        connection.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("<<<snapshotState>>>");

        long checkpointId = context.getCheckpointId();

        List<OutData> outs = pendingUpsertsPerCheckpoint.computeIfAbsent(checkpointId, k -> new ArrayList<>());

        outs.addAll(pendingUpserts);
        System.out.println("snapshotState: " + pendingUpserts);
        pendingUpserts.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("<<<initializeState>>>");
    }
}
