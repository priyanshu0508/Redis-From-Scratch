import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class Main {
    public static void main(String[] args) {
        int port = 6379;
        System.out.println("Logs from your program will appear here!");

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Redis server listening on port " + port + "...");

            // Continuously accept new clients in an infinite loop
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getRemoteSocketAddress());

                // Offload each client to a separate thread
                CompletableFuture.runAsync(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (clientSocket;
             OutputStream out = clientSocket.getOutputStream();
             Scanner scanner = new Scanner(clientSocket.getInputStream())) {

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();

                if (line.toUpperCase().contains("PING")) {
                    out.write("+PONG\r\n".getBytes());
                    out.flush();
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled PING -> Sent +PONG\\r\\n");
                }
            }
        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
        } finally {
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        }
    }
}
