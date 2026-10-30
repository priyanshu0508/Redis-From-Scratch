import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        int port = 6379;
        System.out.println("Logs from your program will appear here!");

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Redis server listening on port " + port + "...");

            // Wait for connection from client
            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());

            // Get the output stream to send data back to the client
            OutputStream out = clientSocket.getOutputStream();

            // Send '+PONG\r\n' (RESP simple string format)
            String pongResponse = "+PONG\r\n";
            out.write(pongResponse.getBytes());
            out.flush();
            System.out.println("Sent: +PONG\\r\\n");

            // Close connection
            clientSocket.close();
            System.out.println("Client connection closed.");
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
