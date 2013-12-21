package spout.dbutils;

import java.sql.*;
import java.text.MessageFormat;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/18/13
 * Time: 6:19 PM
 */


public class SensorDbUtils {

    public static Connection getNewDatabaseConnection (String dbUser, String dbPassword) throws SQLException {
        lookupDriver();
        return connectAndUseDatabase(dbUser, dbPassword);
    }

    private static Connection connectAndUseDatabase (String dbUser, String dbPassword) throws SQLException {
        return DriverManager.getConnection(DB_URL, dbUser, dbPassword);
    }

    private static void lookupDriver () {
        try {
            Class.forName(COM_SQLSERVER_JDBC_DRIVER);
        } catch ( ClassNotFoundException e ) {
            System.out.println("Error: Where is your MySQL JDBC Driver?");
            e.printStackTrace();
        }
    }

    public static int getMaxPollTime (final Connection jdbcConnection, final String tableName) throws SQLException {
        Statement stmt = jdbcConnection.createStatement();
        String sql = MessageFormat.format("SELECT max(polltime) as maxpoll FROM {0}", tableName);
        ResultSet rs = stmt.executeQuery(sql);
        int maxRow = 0;
        if (rs.next()) {
            maxRow = rs.getInt("maxpoll");
        }
        stmt.close();
        rs.close();
        return maxRow;
    }

    public static ResultSet getAllFromSensorDb (final Connection jdbcConnection,
                                                String tableName,
                                                String orderByColumn) throws
    SQLException
    {
	
	System.err.println("DEBUG: Querying sensor DB to stream all data.");

        Statement stmt = jdbcConnection.createStatement();
        stmt.setFetchSize(10000);
        stmt.setQueryTimeout(0);
        String sql = MessageFormat.format("SELECT * FROM {0} WHERE polltime > '2013-12-15' ORDER BY {1}", tableName, orderByColumn);
        return stmt.executeQuery(sql);
    }

    public static final  String TABLE_NAME                = "dbo.sensordata";
    public static final  String SENSOR_COLUMN             = "sensor";
    public static final  String DATA_COLUMN               = "data";
    public static final  String ORDER_BY_COLUMN           = "polltime";
    public static final  String DB_USER                   = "sensorreader";
    private static final String DB_URL                    = "jdbc:sqlserver://zinc14.pha.jhu.edu:1433;Database=owsensordb";
    private static final String COM_SQLSERVER_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    public static final int APPROX_NO_OF_SENSORS = 200;
}
