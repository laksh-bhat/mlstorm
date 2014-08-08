package spout.utils;

import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

 /*
 * Copyright 2013-2015 Lakshmisha Bhat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class MsSqlServerSensorDbUtils {

    public static final String TABLE_NAME = "dbo.sensordata";
    public static final String SENSOR_COLUMN = "sensor";
    public static final String DATA_COLUMN = "data";
    public static final String ORDER_BY_COLUMN = "polltime";
    public static final String DB_USER = "sensorreader";
    public static final String DB_URL = "jdbc:sqlserver://zinc14.pha.jhu.edu:1433;Database=owsensordb";
    public static final int APPROX_NO_OF_SENSORS = 165;
    private static final String PREDICATE = " WHERE polltime > '2013-12-15' ";
    private static final String COM_SQLSERVER_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    public static final Map<Integer, String> buildBiDirectionalSensorDictionary(final Map<String, Integer> sensorDictionary) throws SQLException {
        final Map<Integer, String> reverseDictionary = new ConcurrentSkipListMap<Integer, String>();
        Connection jdbcConnection = getNewDatabaseConnection(DB_USER, DB_USER);
        List<String> sensors = MsSqlServerSensorDbUtils.getSensorsList(jdbcConnection, MsSqlServerSensorDbUtils.TABLE_NAME);
        int sensorIndex = 0;
        for (String sensor : sensors) {
            sensorDictionary.put(sensor, sensorIndex);
            reverseDictionary.put(sensorIndex, sensor);
            sensorIndex++;
        }
        return reverseDictionary;
    }

    private static void cleanupAfterQuery(final ResultSet rs, final Statement stmt) throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
    }

    private static Connection connectAndUseDatabase(String dbUser, String dbPassword) throws SQLException {
        return DriverManager.getConnection(DB_URL, dbUser, dbPassword);
    }

    public static ResultSet getAllFromSensorDb(final Connection jdbcConnection, final String tableName, final String orderByColumn) throws SQLException {
        Logger.getAnonymousLogger().log(Level.INFO, "Querying sensor DB to stream all data.");
        Statement stmt = jdbcConnection.createStatement();
        stmt.setFetchSize(10000);
        stmt.setQueryTimeout(0);
        String sql = MessageFormat.format("SELECT * FROM {0} {1} ORDER BY {2}", tableName, PREDICATE, orderByColumn);
        return stmt.executeQuery(sql);
    }

    public static int getMaxPollTime(final Connection jdbcConnection, final String tableName) throws SQLException {
        int maxRow = 0;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = jdbcConnection.createStatement();
            String sql = MessageFormat.format("SELECT max(polltime) as maxpoll FROM {0}", tableName);
            rs = stmt.executeQuery(sql);
            if (rs.next()) {
                maxRow = rs.getInt("maxpoll");
            }
        } finally {
            cleanupAfterQuery(rs, stmt);
        }
        return maxRow;
    }

    public static Connection getNewDatabaseConnection(String dbUser, String dbPassword) throws SQLException {
        lookupDriver();
        return connectAndUseDatabase(dbUser, dbPassword);
    }

    public static List<String> getSensorsList(final Connection jdbcConnection, String tableName) throws SQLException {
        final List<String> sensors = new ArrayList<String>();
        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = jdbcConnection.createStatement();
            String totalSensors = MessageFormat.format("SELECT DISTINCT(sensor) FROM {0} {1}", tableName, PREDICATE);
            rs = stmt.executeQuery(totalSensors);
            while (rs.next()) {
                sensors.add(rs.getString("sensor"));
            }
        } finally {
            cleanupAfterQuery(rs, stmt);
        }
        Collections.sort(sensors);
        return sensors;
    }

    private static void lookupDriver() {
        try {
            Class.forName(COM_SQLSERVER_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Where is your MySQL JDBC Driver?", e);
        }
    }
}
