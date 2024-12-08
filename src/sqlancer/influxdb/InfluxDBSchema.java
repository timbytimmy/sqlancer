package sqlancer.influxdb;

import java.util.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractRelationalTable;

public class InfluxDBSchema extends AbstractSchema<SQLConnection, InfluxDBSchema.InfluxDBTable> {

    private static final String SHOW_FIELD_KEYS_QUERY = "SHOW FIELD KEYS";
    private static final String SHOW_MEASUREMENTS_QUERY = "SHOW MEASUREMENTS";

    private List<String> databaseMeasurements;

    public enum InfluxDBDataType {
        INTEGER, FLOAT, STRING, BOOLEAN, TIMESTAMP, TAG, FIELD;

        public static InfluxDBDataType getRandomWithoutTime() {
            return Randomly.fromOptions(INTEGER, FLOAT, STRING, BOOLEAN);
        }
    }

    public static class InfluxDBCompositeDataType {
        private final InfluxDBDataType dataType;

        public InfluxDBCompositeDataType(InfluxDBDataType dataType) {
            this.dataType = dataType;
        }

        public InfluxDBDataType getPrimitiveDataType() {
            return dataType;
        }

        @Override
        public String toString() {
            return dataType.name();
        }
    }

    public static class InfluxDBColumn extends AbstractTableColumn<InfluxDBTable, InfluxDBDataType> {
        private final boolean isTag;

        public InfluxDBColumn(String name, InfluxDBDataType columnType, boolean isTag) {
            super(name, null, columnType);
            this.isTag = isTag;
        }

        public boolean isTag() {
            return isTag;
        }
    }

    public static class InfluxDBTable extends AbstractRelationalTable<InfluxDBColumn, Object, SQLConnection> {
        public InfluxDBTable(String tableName, List<InfluxDBColumn> columns) {
            super(tableName, columns, Collections.emptyList(), false);
        }
    }

    public InfluxDBSchema(List<InfluxDBTable> databaseTables, List<String> databaseMeasurements) {
        super(databaseTables);
        this.databaseMeasurements = databaseMeasurements;
    }

    private static InfluxDBDataType getColumnType(String typeString) {
        switch (typeString) {
            case "integer":
                return InfluxDBDataType.INTEGER;
            case "float":
                return InfluxDBDataType.FLOAT;
            case "string":
                return InfluxDBDataType.STRING;
            case "boolean":
                return InfluxDBDataType.BOOLEAN;
            case "time":
                return InfluxDBDataType.TIMESTAMP;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static InfluxDBSchema fromConnection(SQLConnection con, String databaseName) {
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        List<InfluxDBTable> databaseTables = fetchDatabaseTables(influxDB, databaseName);
        List<String> databaseMeasurements = getMeasurementsFromDatabase(influxDB, databaseName);
        return new InfluxDBSchema(databaseTables, databaseMeasurements);
    }

    private static List<InfluxDBTable> fetchDatabaseTables(InfluxDB influxDB, String databaseName) {
        Query query = new Query(SHOW_FIELD_KEYS_QUERY, databaseName);
        QueryResult result = influxDB.query(query);
        List<InfluxDBTable> databaseTables = new ArrayList<>();

        for (QueryResult.Series series : result.getResults().get(0).getSeries()) {
            String tableName = series.getName();
            List<InfluxDBColumn> columns = new ArrayList<>();

            for (List<Object> values : series.getValues()) {
                String columnName = (String) values.get(0);
                String columnTypeString = (String) values.get(1);
                InfluxDBDataType columnType = getColumnType(columnTypeString);
                InfluxDBColumn column = new InfluxDBColumn(columnName, columnType, false);
                columns.add(column);
            }

            InfluxDBTable table = new InfluxDBTable(tableName, columns);
            databaseTables.add(table);
        }

        return databaseTables;
    }

    private static List<String> getMeasurementsFromDatabase(InfluxDB influxDB, String databaseName) {
        List<String> measurements = new ArrayList<>();
        Query query = new Query(SHOW_MEASUREMENTS_QUERY, databaseName);
        QueryResult result = influxDB.query(query);

        for (QueryResult.Series series : result.getResults().get(0).getSeries()) {
            for (List<Object> values : series.getValues()) {
                measurements.add((String) values.get(0));
            }
        }

        return measurements;
    }

    public List<String> getDatabaseMeasurements() {
        return databaseMeasurements;
    }
}