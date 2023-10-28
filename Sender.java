import java.sql.*;

public class Sender {
   public static void main(String args[]) {
      Connection c = ConnectToDatabase("34.76.123.81", "5432", "hw1", "dist_user", "dist_pass_123");
      InsertIntoAsync(c, "Salam Ibo");
      SelectAllFromAsync(c);
   }

   private static Connection ConnectToDatabase(String ip,String port, String databaseName, String user, String password)
   {
      String url = "jdbc:postgresql://" + ip +":" + port + "/" +databaseName;
      Connection c = null;
      try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager
                 .getConnection(url,
                         user, password);
         System.out.println("Opened database in host: " + ip +" successfully!");
         c.setAutoCommit(false);
      } catch (Exception e) {
         System.out.println("Could not connect to database in host: " + ip);
         //e.printStackTrace();
         //System.err.println(e.getClass().getName() + ": " + e.getMessage());
         //System.exit(0);
      }

      return c;
   }

   private static void SelectAllFromAsync(Connection c)
   {
      try{
         Statement statement = c.createStatement();

         String sql = "select * from async_messages;";
         ResultSet rs = statement.executeQuery(sql);
         while ( rs.next() ) {
            int id = rs.getInt("record_id");
            String  name = rs.getString("sender_name");
            String  message = rs.getString("message");
            Timestamp sentTime = rs.getTimestamp("sent_time");
            Timestamp receivedTime = rs.getTimestamp("received_time");
            System.out.println( "ID = " + id );
            System.out.println( "NAME = " + name );
            System.out.println( "Message = " + message );
            System.out.println( "Sent Time = " + sentTime );
            System.out.println( "Received Time = " + receivedTime );
            System.out.println();
         }
         rs.close();
         statement.close();
      }
      catch (SQLException sqlException)
      {
         System.out.println(sqlException.getMessage());
      }
   }

   private static void InsertIntoAsync(Connection c, String message)
   {
      try {
         Statement statement = c.createStatement();
         String sql = "INSERT INTO async_messages (sender_name,message,sent_time) "
                 + "VALUES ('Ayxan', '"+ message+"', CURRENT_TIMESTAMP);";
         statement.executeUpdate(sql);
      }
      catch (SQLException sqlException)
      {
         System.out.println(sqlException.getMessage());
      }
   }

}