package sqlancer.influxdb;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(commandDescription = "InfluxDB")
public class InfluxDBOptions implements DBMSSpecificOptions<InfluxDBOracleFactory> {

    @Parameter(names = "--url", description = "The URL to connect to InfluxDB", required = true)
    public String url = "http://localhost:8086";

    @Parameter(names = "--database-name", description = "The name of the database to use", required = true)
    public String databaseName = "default_db";

    @Parameter(names = "--username", description = "The username to connect to InfluxDB")
    public String username = "root";

    @Parameter(names = "--password", description = "The password to connect to InfluxDB")
    public String password = "root";

    @Parameter(names = "--max-num-measurements", description = "The maximum number of measurements that can be generated for a database", arity = 1)
    public int maxNumMeasurements = 10;

    @Parameter(names = "--max-num-tags", description = "The maximum number of tags that can be generated for a measurement", arity = 1)
    public int maxNumTags = 5;

    @Parameter(names = "--max-num-fields", description = "The maximum number of fields that can be generated for a measurement", arity = 1)
    public int maxNumFields = 10;

    @Parameter(names = "--max-num-queries", description = "The maximum number of queries that are issued for a database", arity = 1)
    public int maxNumQueries = 100;

    @Parameter(names = "--oracle", description = "The list of oracle factories to be used during the tests")
    public List<InfluxDBOracleFactory> oracles = Arrays.asList(InfluxDBOracleFactory.SIMPLE_QUERY);

    @Override
    public List<InfluxDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }
}