import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

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

            OutputStream out = clientSocket.getOutputStream();
            Scanner scanner = new Scanner(clientSocket.getInputStream());

            // Loop to read multiple commands on the same connection
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();

                // Redis commands are case-insensitive
                if (line.toUpperCase().contains("PING")) {
                    out.write("+PONG\r\n".getBytes());
                    out.flush();
                    System.out.println("Handled PING -> Sent +PONG\\r\\n");
                }
            }

            // Connection ends when the client disconnects
            System.out.println("Client disconnected.");
            clientSocket.close();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
