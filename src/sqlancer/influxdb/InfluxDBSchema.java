package sqlancer.influxdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.influxdb.InfluxDBProvider;

public class InfluxDBSchema extends AbstractSchema<InfluxDBProvider.InfluxDBGlobalState, InfluxDBSchema.InfluxDBTable> {

    public enum InfluxDBDataType {
        TIMESTAMP,INT, FLOAT, STRING, BOOLEAN, NULL; // Add other types as necessary

        public static InfluxDBDataType getRandomWithoutNull() {
            InfluxDBDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == InfluxDBDataType.NULL);
            return dt;
        }

    }

    public class InfluxDBCompositeDataType {

        private final InfluxDBDataType dataType;
        private final int size;

        public InfluxDBCompositeDataType(InfluxDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public InfluxDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static InfluxDBCompositeDataType getRandomWithoutNull() {
            InfluxDBDataType type = InfluxDBDataType.getRandomWithoutNull();
            int size = -1;
            switch (type) {
                case TIMESTAMP:
                    size = 0; // InfluxDB stores timestamps in a predefined format
                    break;
                case FLOAT:
                    size = Randomly.fromOptions(4, 8);
                    break;
                case INT:
                    size = Randomly.fromOptions(1, 2, 4, 8);
                    break;
                case STRING:
                    size = 0; // InfluxDB strings do not have a predefined size
                    break;
                case BOOLEAN:
                    size = 0; // Booleans do not have a varying size
                    break;
                default:
                    throw new AssertionError(type);
            }

            return new InfluxDBCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
                case FLOAT:
                    switch (size) {
                        case 8:
                            return Randomly.fromOptions("DOUBLE", "FLOAT8");
                        case 4:
                            return Randomly.fromOptions("FLOAT", "FLOAT4");
                        default:
                            throw new AssertionError(size);
                    }
                case STRING:
                    return "STRING";
                case BOOLEAN:
                    return Randomly.fromOptions("BOOLEAN", "BOOL");
                case TIMESTAMP:
                    return "TIMESTAMP";
                case NULL:
                    return "NULL";
                default:
                    throw new AssertionError(getPrimitiveDataType());
            }
        }
    }

    public class InfluxDBColumn extends AbstractTableColumn<InfluxDBTable, InfluxDBCompositeDataType> {

        private final boolean isNullable;

        public InfluxDBColumn(String name, InfluxDBCompositeDataType columnType, boolean isNullable) {
            super(name, null, columnType); // Replace null with an actual reference to the table if available
            this.isNullable = isNullable;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public class InfluxDBTables extends AbstractTables<InfluxDBTable, InfluxDBColumn> {

        public InfluxDBTables(List<InfluxDBTable> tables) {
            super(tables);
        }
    }

    public InfluxDBSchema(List<InfluxDBTable> databaseTables) {
        super(databaseTables);
    }

    public InfluxDBTables getRandomNonEmptyTables() {
        return new InfluxDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static InfluxDBCompositeDataType getColumnType(String typeString) {
        InfluxDBDataType primitiveType;
        int size = -1;

        switch (typeString) {
            case "INTEGER":
            case "INT":
                primitiveType = InfluxDBDataType.INT;
                size = 4;
                break;
            case "BIGINT":
                primitiveType = InfluxDBDataType.INT;
                size = 8;
                break;
            case "SMALLINT":
                primitiveType = InfluxDBDataType.INT;
                size = 2;
                break;
            case "TINYINT":
                primitiveType = InfluxDBDataType.INT;
                size = 1;
                break;
            case "FLOAT":
                primitiveType = InfluxDBDataType.FLOAT;
                size = 4;
                break;
            case "DOUBLE":
                primitiveType = InfluxDBDataType.FLOAT;
                size = 8;
                break;
            case "BOOLEAN":
                primitiveType = InfluxDBDataType.BOOLEAN;
                break;
            case "STRING":
            case "VARCHAR":
                primitiveType = InfluxDBDataType.STRING;
                break;
            case "TIMESTAMP":
                primitiveType = InfluxDBDataType.TIMESTAMP;
                break;
            case "\"NULL\"":
                primitiveType = InfluxDBDataType.NULL;
                break;
            default:
                throw new AssertionError("Unknown type: " + typeString);
        }

        return new InfluxDBCompositeDataType(primitiveType, size);
    }

    public class InfluxDBTable extends AbstractRelationalTable<InfluxDBColumn, TableIndex, InfluxDBGlobalState> {

        public InfluxDBTable(String tableName, List<InfluxDBColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public InfluxDBSchema fromConnection(Connection con, String databaseName) throws SQLException {
        List<InfluxDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con, databaseName);

        for (String tableName : tableNames) {
                        List<InfluxDBColumn> databaseColumns = getTableColumns(con, tableName);

            boolean isView = tableName.startsWith("v");
            InfluxDBTable table = new InfluxDBTable(tableName, databaseColumns, isView);

            for (InfluxDBColumn column : databaseColumns) {
                column.setTable(table);
            }

            databaseTables.add(table);
        }

        return new InfluxDBSchema(databaseTables);
    }

    private static List<String> getTableNames(Connection con, String databaseName) throws SQLException {
        List<String> tableNames = new ArrayList<>();

        try (Statement statement = con.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW MEASUREMENTS")) { //use SHOW MEASUREMENT to handle influxQL
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(1));  //first column contains the measurement name
            }
        }

        return tableNames;
    }

    private List<InfluxDBColumn> getTableColumns(Connection con, String tableName) throws SQLException {
        List<InfluxDBColumn> columns = new ArrayList<>();

        // Fetching field keys (data columns)
        try (Statement statement = con.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW FIELD KEYS FROM " + tableName)) {

            while (resultSet.next()) {
                String columnName = resultSet.getString("fieldKey");
                String columnTypeString = resultSet.getString("fieldType");
                InfluxDBCompositeDataType columnType = InfluxDBSchema.getColumnType(columnTypeString);
                boolean isNullable = true; // By default, this fields in InfluxDB are nullable
                // In InfluxDB, PRIMARY KEY concept is different (tags + timestamp form the primary key generally)
                InfluxDBColumn column = new InfluxDBColumn(columnName, columnType, isNullable);
                columns.add(column);
            }
        }
        // Fetching tag keys (index columns)
        try (Statement statement = con.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW TAG KEYS FROM " + tableName)) {

            while (resultSet.next()) {
                String columnName = resultSet.getString("tagKey");
                   InfluxDBCompositeDataType columnType = new InfluxDBCompositeDataType(InfluxDBDataType.STRING, -1);
                boolean isNullable = false; // Tags are generally not nullable
                InfluxDBColumn column = new InfluxDBColumn(columnName, columnType, isNullable);
                columns.add(column);
            }
        }

        return columns;
    }





}
