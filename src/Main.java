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
    // Record to store a value along with an optional expiration timestamp (in milliseconds)
    record ValueEntry(String value, Long expiresAt) {
        boolean isExpired() {
            return expiresAt != null && System.currentTimeMillis() > expiresAt;
        }
    }

    // Thread-safe in-memory key-value database supporting TTL expiration
    private static final Map<String, ValueEntry> database = new ConcurrentHashMap<>();

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
                Long expiresAt = null;

                // Check for 'PX <milliseconds>' argument
                for (int i = 3; i < tokens.size(); i++) {
                    if (tokens.get(i).equalsIgnoreCase("PX") && i + 1 < tokens.size()) {
                        long ttlMs = Long.parseLong(tokens.get(i + 1));
                        expiresAt = System.currentTimeMillis() + ttlMs;
                        break;
                    }
                }

                database.put(key, new ValueEntry(value, expiresAt));
                out.write("+OK\r\n".getBytes());
                out.flush();
                String ttlLog = (expiresAt != null) ? " (expires in " + (expiresAt - System.currentTimeMillis()) + "ms)" : "";
                System.out.println("[" + Thread.currentThread().getName() + "] Handled SET " + key + " = " + value + ttlLog);
            }
            case "GET" -> {
                String key = tokens.get(1);
                ValueEntry entry = database.get(key);

                // Passive / Lazy expiration: remove from map if expired
                if (entry != null && entry.isExpired()) {
                    database.remove(key);
                    entry = null;
                }

                if (entry != null) {
                    // Key found & active: return Bulk String
                    String val = entry.value();
                    String response = "$" + val.length() + "\r\n" + val + "\r\n";
                    out.write(response.getBytes());
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled GET " + key + " -> " + val);
                } else {
                    // Key not found or expired: return RESP Null Bulk String ($-1\r\n)
                    out.write("$-1\r\n".getBytes());
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled GET " + key + " -> (nil)");
                }
                out.flush();
            }
            case "EXISTS" -> {
                String key = tokens.get(1);
                ValueEntry entry = database.get(key);
                if (entry != null && entry.isExpired()) {
                    database.remove(key);
                    entry = null;
                }
                int exists = (entry != null) ? 1 : 0;
                // RESP Integer format: :<number>\r\n
                out.write((":" + exists + "\r\n").getBytes());
                out.flush();
                System.out.println("[" + Thread.currentThread().getName() + "] Handled EXISTS " + key + " -> " + exists);
            }
            case "DEL" -> {
                int deletedCount = 0;
                for (int i = 1; i < tokens.size(); i++) {
                    String key = tokens.get(i);
                    ValueEntry removed = database.remove(key);
                    if (removed != null && !removed.isExpired()) {
                        deletedCount++;
                    }
                }
                // RESP Integer format: :<count>\r\n
                out.write((":" + deletedCount + "\r\n").getBytes());
                out.flush();
                System.out.println("[" + Thread.currentThread().getName() + "] Handled DEL -> deleted " + deletedCount + " key(s)");
            }
            case "KEYS" -> {
                // Purge expired keys and gather active keys
                List<String> activeKeys = new ArrayList<>();
                for (Map.Entry<String, ValueEntry> e : database.entrySet()) {
                    if (e.getValue().isExpired()) {
                        database.remove(e.getKey());
                    } else {
                        activeKeys.add(e.getKey());
                    }
                }
                // RESP Array of Bulk Strings format: *<count>\r\n$<len>\r\n<key>\r\n...
                StringBuilder resp = new StringBuilder();
                resp.append("*").append(activeKeys.size()).append("\r\n");
                for (String k : activeKeys) {
                    resp.append("$").append(k.length()).append("\r\n").append(k).append("\r\n");
                }
                out.write(resp.toString().getBytes());
                out.flush();
                System.out.println("[" + Thread.currentThread().getName() + "] Handled KEYS -> returned " + activeKeys.size() + " key(s)");
            }
            default -> {
                String error = "-ERR unknown command '" + command + "'\r\n";
                out.write(error.getBytes());
                out.flush();
            }
        }
    }
}
