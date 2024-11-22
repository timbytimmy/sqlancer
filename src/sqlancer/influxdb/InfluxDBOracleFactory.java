package sqlancer.influxdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.duckdb.DuckDBProvider;
import sqlancer.influxdb.gen.InfluxDBExpressionGenerator;
import sqlancer.influxdb.gen.InfluxDBQueryPartitioningHavingTester;
import sqlancer.influxdb.test.InfluxDBAggregationTester;
import sqlancer.influxdb.test.InfluxDBDownSamplingTester;
import sqlancer.influxdb.test.InfluxDBContinuousQueryTester;
import sqlancer.influxdb.test.InfluxDBRetentionPolicyTester;
import sqlancer.influxdb.test.InfluxDBQueryTester;


public enum InfluxDBOracleFactory implements OracleFactory<InfluxDBProvider.InfluxDBGlobalState>{
    HAVING {
        @Override
        public TestOracle<InfluxDBProvider.InfluxDBGlobalState> create(InfluxDBProvider.InfluxDBGlobalState globalState)
                throws SQLException {
            return new InfluxDBQueryPartitioningHavingTester(globalState);
        }
    }




}
