import java.sql.Connection;
import java.sql.DriverManager;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-26 11:49
 **/
public class Test {

    public static void main(String[] args) {
        Connection conn = null;

        try {
            String userName = "root";
            String password = "000000";

            String url = "jdbc:mysql://8xj0202144.zicp.fun:54723/edu_config?useSSL=false";
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(url, userName, password);
            System.out.println("Database connection established");
        } catch (Exception e) {
            System.err.println("Cannot connect to database server");
            System.err.println(e.getMessage());
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                    System.out.println("Database Connection Terminated");
                } catch (Exception e) {}
            }
        }
    }
}
