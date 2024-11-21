package sqlancer.influxdb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.influxdb.InfluxDBProvider.InfluxDBGlobalState;
import sqlancer.influxdb.gen.InfluxDBMeasurementGenerator;
import sqlancer.influxdb.gen.InfluxDBInsertGenerator;
import sqlancer.influxdb.gen.InfluxDBRandomQuerySynthesizer;
import sqlancer.influxdb.gen.InfluxDBShowGenerator;
import sqlancer.influxdb.gen.InfluxDBQueryGenerator;

@AutoService(DatabaseProvider.class)
public class InfluxDBProvider extends SQLProviderAdapter<InfluxDBGlobalState, InfluxDBOptions> {

    public InfluxDBProvider() {
        super(InfluxDBProvider.InfluxDBGlobalState.class, InfluxDBOptions.class);
    }

    public enum Action implements AbstractAction<InfluxDBGlobalState> {

        INSERT(InfluxDBInsertGenerator::getQuery),
        SHOW_MEASUREMENTS((g) -> new SQLQueryAdapter("SHOW MEASUREMENTS;")), //
        SHOW_SERIES((g) -> new SQLQueryAdapter("SHOW SERIES;")),
        DELETE(InfluxDBDeleteGenerator::generate),
        CREATE_CONTINUOUS_QUERY(InfluxDBContinuousQueryGenerator::getQuery),
        QUERY(InfluxDBQueryGenerator::generateSelectWithConditions),

        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            InfluxDBErrors.addExpressionErrors(errors);
            InfluxDBErrors.addGroupByErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + InfluxDBToStringVisitor
                            .asString(InfluxDBRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<InfluxDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<InfluxDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(InfluxDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static int mapInfluxDBActions(InfluxDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
            case INSERT:
                return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            case SHOW_MEASUREMENTS:
                return 1; // show queries are run less frequently or just once.
            case SHOW_SERIES:
                return 1; // Same as SHOW_MEASUREMENTS.
            case DELETE:
                return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
            case CREATE_CONTINUOUS_QUERY:
                return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumContinuousQueries + 1);
            case QUERY:
                return r.getInteger(0, globalState.getOptions().getMaxNumberQueries());
            case EXPLAIN:
                return r.getInteger(0, 2);
            default:
                throw new AssertionError(a);
        }
    }

    public class InfluxDBGlobalState extends SQLGlobalState<InfluxDBOptions, InfluxDBSchema> {

        @Override
        protected InfluxDBSchema readSchema() throws SQLException {
            return InfluxDBSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }

    @Override
    public void generateDatabase(InfluxDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new InfluxDBMeasurementGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getMeasurements().isEmpty()) {
            throw new IgnoreMeException(); // Ensure there is at least one measurement
        }
        StatementExecutor<InfluxDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                InfluxDBProvider::mapInfluxDBActions, (q) -> {
            if (globalState.getSchema().getMeasurements().isEmpty()) {
                throw new IgnoreMeException(); // Skip actions if no measurements
            }
        });
        se.executeStatements();
    }

    private static final Logger LOGGER = Logger.getLogger(InfluxDBProvider.class.getName());

    public void tryDeleteFile(String fname) {
        try {
            File f = new File(fname);
            f.delete();
        } catch (Exception e) {
        }
    }

    public void tryDeleteDatabase(String dbpath) {
        if (dbpath.equals("") || dbpath.equals(":memory:")) {
            return;
        }
        tryDeleteFile(dbpath);
        tryDeleteFile(dbpath + ".wal");
    }

    @Override
    public SQLConnection createDatabase(InfluxDBProvider.InfluxDBGlobalState globalState) throws SQLException {
        String databaseFile = System.getProperty("influxdb.database.file", "");
        String url = "http://localhost:8086" + databaseFile;
        tryDeleteDatabase(databaseFile);

        MainOptions options = globalState.getOptions();
        if (!(options.isDefaultUsername() && options.isDefaultPassword())) {
            throw new AssertionError("InfluxDB doesn't support credentials (username/password)");
        }

        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("PRAGMA checkpoint_threshold='1 byte';");
        stmt.close();
        return new SQLConnection(conn);
    }

    @Override
    public String getDBMSName() {
        return "influxdb";
    }

}









}
