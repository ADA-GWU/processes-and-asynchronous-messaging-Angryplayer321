import java.sql.*;
import java.util.Scanner;
import java.util.concurrent.*;


public class Sender {
   public static void main(String args[]) {
      Connection c = ConnectToDatabase("34.76.123.81", "5432", "hw1", "dist_user", "dist_pass_123");
      InsertIntoAsync(c, "Shukur");
      InsertIntoAsync(c, "Salamatciliq");
      ExecutorService executor = Executors.newFixedThreadPool(1);
      CompletableFuture<Void> senderFuture = CompletableFuture.runAsync(() -> {
         // Code for sending the message (replace this with your actual sending logic)
         while(!FetchMessage(c, true))
         {
            try {
               TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
      }, executor);

//      while(!FetchMessage(c))
//      {
//         try {
//            TimeUnit.SECONDS.sleep(2);
//         } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//         }
//      }
      SelectAllFromAsync(c);
      Scanner sc = new Scanner(System.in);
      System.out.println("Enter the message:");

      while (true) {
         String s =sc.nextLine();
         if(s.equals("Exit")) {
            executor.shutdown();
            break;
         }
         else InsertIntoAsync(c, s);
      }
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
   private static boolean FetchMessage(Connection c,boolean isEndless)
   {
      boolean isAllFetched = true;
      try{
         Statement statement = c.createStatement();

         String sql = "select * from async_messages where received_time is null;";
         ResultSet rs = statement.executeQuery(sql);
         while ( rs.next() ) {
            int id = rs.getInt("record_id");
            String  name = rs.getString("sender_name");
            String  message = rs.getString("message");
            Timestamp sentTime = rs.getTimestamp("sent_time");
            Timestamp receivedTime = rs.getTimestamp("received_time");
            if(receivedTime==null) {
               isAllFetched = false;
               System.out.println(name + ": " + message);
               System.out.println();
               UpdateMessage(c, id);
               break;
            }
         }
         rs.close();
         statement.close();
//         if(isAllFetched) System.out.println("There is no messages.");
         if(isEndless) return false;
         return isAllFetched;
      }
      catch (SQLException sqlException)
      {
         System.out.println(sqlException.getMessage());
      }
      return false;
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
   private static void UpdateMessage(Connection c, int id)
   {
      try {
         Statement statement = c.createStatement();
         String sql = "UPDATE async_messages set received_time = current_timestamp where record_id="+id+";";
         statement.executeUpdate(sql);
      }
      catch (SQLException sqlException)
      {
         System.out.println(sqlException.getMessage());
      }
   }

}