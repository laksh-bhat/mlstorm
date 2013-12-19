package utils;

/**
 * User: lbhat <laksh85@gmail.com>
 * Date: 12/18/13
 * Time: 5:21 PM
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

public class TestJdbcConnection {
    public void dbConnect (String dbConnectString,
                           String dbUserid,
                           String dbPassword)
    {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection conn = DriverManager.getConnection(dbConnectString,dbUserid, dbPassword);
            System.out.println("connected");
            Statement statement = conn.createStatement();
            String queryString = "" +
                    " select * " +
                    " from dbo.sensorData " +
                    " where polltime > '2013-12-15' order by polltime"
            ;
            ResultSet rs = statement.executeQuery(queryString);
            while (rs.next()) {
                int i = rs.getMetaData().getColumnCount();
                while (i > 0)
                    System.out.print(MessageFormat.format("{0} ", rs.getString(i--)));
                System.out.println();
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
        TestJdbcConnection connServer = new TestJdbcConnection();
        connServer.dbConnect("jdbc:sqlserver://zinc14.pha.jhu.edu:1433;Database=owsensordb", "sensorreader","sensorreader");
    }
}
