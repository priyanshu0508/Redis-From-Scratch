import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    // Thread-safe in-memory key-value database
    private static final Map<String, String> database = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        int port = 6379;
        System.out.println("Logs from your program will appear here!");

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Redis server listening on port " + port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getRemoteSocketAddress());
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
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;

                List<String> tokens = new ArrayList<>();

                // If line starts with '*', it is a RESP Array (standard Redis wire protocol)
                if (line.startsWith("*")) {
                    int count = Integer.parseInt(line.substring(1));
                    for (int i = 0; i < count; i++) {
                        scanner.nextLine(); // Skip the '$<length>' header
                        tokens.add(scanner.nextLine()); // Read the actual argument string
                    }
                } else {
                    // Simple inline commands (e.g. "PING", "ECHO hello", "SET k v", "GET k")
                    tokens.addAll(Arrays.asList(line.split("\\s+")));
                }

                if (tokens.isEmpty()) continue;
                processCommand(tokens, out);
            }
        } catch (Exception e) {
            System.out.println("Client disconnected or error: " + e.getMessage());
        } finally {
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        }
    }

    private static void processCommand(List<String> tokens, OutputStream out) throws IOException {
        String command = tokens.get(0).toUpperCase();

        switch (command) {
            case "PING" -> {
                out.write("+PONG\r\n".getBytes());
                out.flush();
                System.out.println("[" + Thread.currentThread().getName() + "] Handled PING -> +PONG");
            }
            case "ECHO" -> {
                String message = tokens.get(1);
                String response = "$" + message.length() + "\r\n" + message + "\r\n";
                out.write(response.getBytes());
                out.flush();
                System.out.println("[" + Thread.currentThread().getName() + "] Handled ECHO -> " + message);
            }
            case "SET" -> {
                String key = tokens.get(1);
                String value = tokens.get(2);
                database.put(key, value);
                out.write("+OK\r\n".getBytes());
                out.flush();
                System.out.println("[" + Thread.currentThread().getName() + "] Handled SET " + key + " = " + value);
            }
            case "GET" -> {
                String key = tokens.get(1);
                String value = database.get(key);
                if (value != null) {
                    // Key found: return Bulk String
                    String response = "$" + value.length() + "\r\n" + value + "\r\n";
                    out.write(response.getBytes());
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled GET " + key + " -> " + value);
                } else {
                    // Key not found: return RESP Null Bulk String ($-1\r\n)
                    out.write("$-1\r\n".getBytes());
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled GET " + key + " -> (nil)");
                }
                out.flush();
            }
            default -> {
                String error = "-ERR unknown command '" + command + "'\r\n";
                out.write(error.getBytes());
                out.flush();
            }
        }
    }
}
