import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        int port = 6379;
        System.out.println("Logs from your program will appear here!");

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors.
            serverSocket.setReuseAddress(true);
            System.out.println("Redis server listening on port " + port + "...");

            // Wait for connection from client
            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());

            // Close connection for Step 1
            clientSocket.close();
            System.out.println("Client connection closed.");
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
