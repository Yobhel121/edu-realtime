import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-31 17:07
 **/
public class PhoenixCreateTableExample {
    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            String jdbcUrl = "jdbc:phoenix:localhost:2181:/hbase";
            connection = DriverManager.getConnection(jdbcUrl);
            statement = connection.createStatement();

            String createTableSQL = "CREATE TABLE IF NOT EXISTS  test_user3("
                    + "id INTEGER PRIMARY KEY,"
                    + "uname VARCHAR)";
            statement.execute(createTableSQL);

            System.out.println("Table created successfully.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
