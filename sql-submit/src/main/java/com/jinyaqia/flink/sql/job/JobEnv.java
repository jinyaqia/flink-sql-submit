package com.jinyaqia.flink.sql.job;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * 定义一个flink sql作业
 *
 * @author jinyaqia
 * @date 2019-12-09 16:18
 */
@Data
@Slf4j
public class JobEnv {
    private EnvironmentSettings settings;
    private StreamExecutionEnvironment stEnv;
    private TableEnvironment tEnv;
    private StatementSet stmtSet;

    public JobEnv() {
        settings = EnvironmentSettings.newInstance().build();
        stEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(stEnv, settings);
        stEnv.getConfig().enableObjectReuse();
        stEnv.getConfig().setAutoWatermarkInterval(200);
        stEnv.getConfig().setLatencyTrackingInterval(1000);
        stEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        stEnv.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        tEnv.getConfig().getConfiguration().setString(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key(), "true");
        stEnv.enableCheckpointing(30000);
        stmtSet = tEnv.createStatementSet();
    }

    public void run(String jobName) throws Exception {
        if (StringUtils.isNotBlank(jobName)) {
            tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, jobName);
        }
        TableResult tableResult = stmtSet.execute();
        tableResult.getJobClient().ifPresent(el -> {
            log.info("jobId={}", el.getJobID().toString());
        });
    }

    public TableResult executeSql(String sql) {
        return tEnv.executeSql(sql);
    }

    public void addInsert(String sql) {
        stmtSet.addInsertSql(sql);
    }
    public String explain(){
        return stmtSet.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE);
    }
}
