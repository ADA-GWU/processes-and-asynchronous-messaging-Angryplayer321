import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;


public class Messenger extends Thread {


   public static ArrayList<Connection> connections = new ArrayList<Connection>();
   public static boolean isMessageFound = false;
   Connection threadConnection;
   public Messenger(Connection threadConnection) {
      this.threadConnection = threadConnection;
   }

   public static void main(String args[]) {

      connections.add(ConnectToDatabase("34.76.123.81", "5432", "hw1", "dist_user", "dist_pass_123"));
      connections.add(ConnectToDatabase("34.75.144.18", "5432", "hw1", "dist_user", "dist_pass_123"));
      ArrayList<SenderThread> senderThreads = PrepareSender();
      SendMessage(senderThreads, "Salam ayqa");
      Thread thread = new Thread(() -> {
         ManageDatabases(connections);
      });
      thread.start();

      Scanner sc = new Scanner(System.in);


      //System.out.println("Enter the message:");


      while (true) {
         String s = sc.nextLine();
         SendMessage(senderThreads, s);
      }
   }

   private static Connection ConnectToDatabase(String ip, String port, String databaseName, String user, String password) {
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

   private static void SelectAllFromAsync(Connection c) {
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
      } catch (SQLException sqlException) {
         System.out.println(sqlException.getMessage());
      }
   }

   private static boolean FetchMessage(Connection c,boolean isEndless) {
      boolean isAllFetched = true;
      try{
         Statement statement = c.createStatement();

         String sql = "select * from async_messages where received_time is null and sender_name != 'Aykhan' limit 1 for update;";
         ResultSet rs = statement.executeQuery(sql);
         while ( rs.next() ) {
            int id = rs.getInt("record_id");
            String name = rs.getString("sender_name");
            String message = rs.getString("message");
            Timestamp sentTime = rs.getTimestamp("sent_time");
            Timestamp receivedTime = rs.getTimestamp("received_time");
            if (receivedTime == null && !isMessageFound) {
               isMessageFound = true;
               isAllFetched = false;
               System.out.println("Sender "+name + " sent " + message + " at time: " + sentTime);
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
      } catch (SQLException sqlException) {
         System.out.println(sqlException.getMessage());
      }
      return false;
   }

   public static void InsertIntoAsync(Connection c, String message) {
      try {
         Statement statement = c.createStatement();
         String sql = "INSERT INTO async_messages (sender_name,message,sent_time) "
                 + "VALUES ('Aykhan', '" + message + "', CURRENT_TIMESTAMP);";
         statement.executeUpdate(sql);
      } catch (SQLException sqlException) {
         System.out.println(sqlException.getMessage());
      }
   }

   private static void UpdateMessage(Connection c, int id) {
      try {
         Statement statement = c.createStatement();
         String sql = "UPDATE async_messages set received_time = current_timestamp where record_id=" + id + ";";
         statement.executeUpdate(sql);
      } catch (SQLException sqlException) {
         System.out.println(sqlException.getMessage());
      }
   }

   private static void ManageDatabases(ArrayList<Connection> connections) {
      ArrayList<Messenger> threads = new ArrayList<Messenger>();
      for (Connection connection : connections) {
         threads.add(new Messenger(connection));
      }
      while (true) {
         try {
            TimeUnit.SECONDS.sleep(2);

            for (Messenger thread : threads) {
               thread.start();
               //System.out.println("Threads go brrr....");
            }
            for (int i = 0; i < threads.size(); i++) {
               threads.get(i).join();
               threads.set(i, new Messenger(threads.get(i).threadConnection));
            }
            isMessageFound = false;
         } catch (Exception e) {
            System.out.println(e);
         }
      }
   }

   public static ArrayList<SenderThread> PrepareSender() {
      ArrayList<SenderThread> senderThreads = new ArrayList<SenderThread>();
      for (Connection connection : connections) {
         senderThreads.add(new SenderThread(connection));
         senderThreads.get(senderThreads.size() - 1).start();
      }
      return senderThreads;
   }

   public static void SendMessage(ArrayList<SenderThread> senderThreads, String message) {
      SenderThread thread = senderThreads.get(RandomGenerator.getDefault().nextInt() % senderThreads.size());
      thread.message = message;
      thread.isWaiting = false;
   }

   public void run() {
      //System.out.println("salam");
      FetchMessage(threadConnection, false);

   }
}

class SenderThread extends Thread {
   public Connection c;
   public String message;
   public boolean isWaiting = true;

   public SenderThread(Connection c) {
      this.c = c;
   }

   public void setMessage(String message) {
      this.message = message;
   }

   public void setWaiting(boolean waiting) {
      isWaiting = waiting;
   }

   public void run() {
      while (true) {
         try {
            TimeUnit.NANOSECONDS.sleep(1);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
         if (!isWaiting) {
            Messenger.InsertIntoAsync(c, message);
            isWaiting = true;
         }

      }
   }
}

