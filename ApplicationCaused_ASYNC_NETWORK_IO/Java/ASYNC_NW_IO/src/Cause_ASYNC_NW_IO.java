import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Cause_ASYNC_NW_IO {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar Cause_ASYNC_NW_IO.jar <JDBC_URL> <USERNAME> <PASSWORD>");
            return;
        }

        String jdbcUrl = args[0];
        String username = args[1];
        String password = args[2];

        try (
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT SalesOrderID, CarrierTrackingNumber, rowguid FROM Sales.SalesOrderDetail;");
            ResultSet resultSet = preparedStatement.executeQuery();            
        ) {
            while (resultSet.next()) {
                int id = resultSet.getInt("SalesOrderID");
                String TrackingNumber = resultSet.getString("CarrierTrackingNumber");
                String rowguid = resultSet.getString("rowguid");

                System.out.println("Sales Order ID: " + id + ", Carrier Tracking Number: " + TrackingNumber + ", Row GUID: " + rowguid);

                try {
               // wait for 1 seconds before going to next record
               Thread.sleep(1000);
               } catch (InterruptedException e){
                   Thread.currentThread().interrupt();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
