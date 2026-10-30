import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Main {
    // Record to store a value along with an optional expiration timestamp (in milliseconds)
    record ValueEntry(String value, Long expiresAt) {
        boolean isExpired() {
            return expiresAt != null && System.currentTimeMillis() > expiresAt;
        }
    }

    // Thread-safe in-memory key-value database supporting TTL expiration
    private static final Map<String, ValueEntry> database = new ConcurrentHashMap<>();

    // Thread-safe in-memory list database for LPUSH, RPUSH, LRANGE
    private static final Map<String, Deque<String>> listDatabase = new ConcurrentHashMap<>();

    // Thread-safe in-memory hash database for HSET, HGET, HGETALL
    private static final Map<String, Map<String, String>> hashDatabase = new ConcurrentHashMap<>();

    // Thread-safe list of all connected replica output streams for command propagation
    private static final List<OutputStream> replicaStreams = new java.util.concurrent.CopyOnWriteArrayList<>();

    // Server configuration parameters (default values can be overridden via CLI flags)
    private static int port = 6379;
    private static String dir = "/tmp/redis-data";
    private static String dbfilename = "dump.rdb";

    // Replication metadata
    private static String role = "master";
    private static String masterHost = null;
    private static int masterPort = 0;
    private static final String masterReplId = "8371b4fb1155b71f4a04d3b1bc3e18c50f415a69";
    private static final int masterReplOffset = 0;

    public static void main(String[] args) {
        // Parse CLI arguments: --port <port> --dir <dir> --dbfilename <file> --replicaof "<host> <port>"
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("--dir") && i + 1 < args.length) {
                dir = args[++i];
            } else if (args[i].equalsIgnoreCase("--dbfilename") && i + 1 < args.length) {
                dbfilename = args[++i];
            } else if (args[i].equalsIgnoreCase("--replicaof") && i + 1 < args.length) {
                role = "slave";
                String replicaArg = args[++i];
                String[] parts = replicaArg.split("\\s+");
                masterHost = parts[0];
                masterPort = Integer.parseInt(parts[1]);
            }
        }

        System.out.println("Logs from your program will appear here!");
        System.out.println("Configuration: port = " + port + ", role = " + role + ", dir = " + dir + ", dbfilename = " + dbfilename);

        // If running as a replica, initiate the handshake with the master server
        if (role.equals("slave") && masterHost != null) {
            initiateReplicaHandshake(masterHost, masterPort, port);
        }

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
        boolean inTransaction = false;
        List<List<String>> transactionQueue = new ArrayList<>();
        OutputStream out = null;

        try {
            out = clientSocket.getOutputStream();
            Scanner scanner = new Scanner(clientSocket.getInputStream());

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
                    // Simple inline commands (e.g. "PING", "MULTI", "EXEC", etc.)
                    tokens.addAll(Arrays.asList(line.split("\\s+")));
                }

                if (tokens.isEmpty()) continue;

                String command = tokens.get(0).toUpperCase();

                // If the client is a replica sending PSYNC, register for propagation
                // and CONTINUE the loop so the connection remains open
                if (command.equals("PSYNC")) {
                    String response = executeCommand(tokens);
                    out.write(response.getBytes());
                    out.flush();
                    replicaStreams.add(out);
                    System.out.println("[Master] Replica registered for propagation. Total active replicas: " + replicaStreams.size());
                    continue;
                }

                // Transaction handling: MULTI, EXEC, DISCARD
                if (command.equals("MULTI")) {
                    inTransaction = true;
                    transactionQueue.clear();
                    out.write("+OK\r\n".getBytes());
                    out.flush();
                    System.out.println("[" + Thread.currentThread().getName() + "] Transaction started (MULTI)");
                } else if (command.equals("DISCARD")) {
                    if (!inTransaction) {
                        out.write("-ERR DISCARD without MULTI\r\n".getBytes());
                    } else {
                        inTransaction = false;
                        transactionQueue.clear();
                        out.write("+OK\r\n".getBytes());
                        System.out.println("[" + Thread.currentThread().getName() + "] Transaction discarded (DISCARD)");
                    }
                    out.flush();
                } else if (command.equals("EXEC")) {
                    if (!inTransaction) {
                        out.write("-ERR EXEC without MULTI\r\n".getBytes());
                    } else {
                        inTransaction = false;
                        // Return RESP Array with results of all queued commands
                        StringBuilder execResp = new StringBuilder();
                        execResp.append("*").append(transactionQueue.size()).append("\r\n");
                        for (List<String> queuedCmd : transactionQueue) {
                            execResp.append(executeCommand(queuedCmd));
                        }
                        transactionQueue.clear();
                        out.write(execResp.toString().getBytes());
                        System.out.println("[" + Thread.currentThread().getName() + "] Executed transaction block (EXEC)");
                    }
                    out.flush();
                } else {
                    // Regular command: queue if in transaction, else execute immediately
                    if (inTransaction) {
                        transactionQueue.add(tokens);
                        out.write("+QUEUED\r\n".getBytes());
                        out.flush();
                        System.out.println("[" + Thread.currentThread().getName() + "] Queued: " + command);
                    } else {
                        String response = executeCommand(tokens);
                        out.write(response.getBytes());
                        out.flush();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Client disconnected or error: " + e.getMessage());
        } finally {
            if (out != null) {
                replicaStreams.remove(out);
            }
            try {
                clientSocket.close();
            } catch (IOException ignored) {}
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        }
    }

    private static void initiateReplicaHandshake(String host, int masterPort, int replicaPort) {
        CompletableFuture.runAsync(() -> {
            try {
                // Short wait to ensure master is ready
                Thread.sleep(600);
                System.out.println("[Replica] Connecting to master at " + host + ":" + masterPort + "...");
                Socket masterSocket = new Socket(host, masterPort);
                OutputStream out = masterSocket.getOutputStream();
                Scanner in = new Scanner(masterSocket.getInputStream());

                // Handshake 1: Send PING
                out.write("*1\r\n$4\r\nPING\r\n".getBytes());
                out.flush();
                String pingResp = in.nextLine();
                System.out.println("[Replica Handshake 1/3] Sent PING -> Master replied: " + pingResp);

                // Handshake 2: Send REPLCONF listening-port <replicaPort>
                String portStr = String.valueOf(replicaPort);
                String conf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + portStr + "\r\n";
                out.write(conf1.getBytes());
                out.flush();
                String confResp1 = in.nextLine();

                // Send REPLCONF capa psync2
                String conf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                out.write(conf2.getBytes());
                out.flush();
                String confResp2 = in.nextLine();
                System.out.println("[Replica Handshake 2/3] Sent REPLCONF -> Master replied: " + confResp1 + " and " + confResp2);

                // Handshake 3: Send PSYNC ? -1
                String psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                out.write(psync.getBytes());
                out.flush();
                String psyncResp = in.nextLine();
                System.out.println("[Replica Handshake 3/3] Sent PSYNC ? -1 -> Master replied: " + psyncResp);
                System.out.println("[Replica] === HANDSHAKE SUCCESSFULLY COMPLETED! ===");

                // Continuous loop: listen for commands propagated from Master
                while (in.hasNextLine()) {
                    String line = in.nextLine().trim();
                    if (line.isEmpty()) continue;

                    List<String> tokens = new ArrayList<>();
                    if (line.startsWith("*")) {
                        int count = Integer.parseInt(line.substring(1));
                        for (int i = 0; i < count; i++) {
                            in.nextLine(); // skip $<length>
                            tokens.add(in.nextLine()); // token
                        }
                    } else {
                        tokens.addAll(Arrays.asList(line.split("\\s+")));
                    }

                    if (tokens.isEmpty()) continue;

                    String cmd = tokens.get(0).toUpperCase();
                    System.out.println("[Replica] Received propagated command: " + cmd + " " + String.join(" ", tokens.subList(1, tokens.size())));

                    // Execute into replica's local database
                    executeCommand(tokens);
                }

            } catch (Exception e) {
                System.out.println("[Replica] Handshake error: " + e.getMessage());
            }
        });
    }

    /**
     * Propagates a write command to all connected replicas in RESP Array format.
     * Called after every successful SET, INCR, DEL on the master.
     */
    private static void propagateToReplicas(List<String> tokens) {
        if (replicaStreams.isEmpty()) return;

        // Encode command as RESP Array
        StringBuilder resp = new StringBuilder();
        resp.append("*").append(tokens.size()).append("\r\n");
        for (String token : tokens) {
            resp.append("$").append(token.length()).append("\r\n").append(token).append("\r\n");
        }
        byte[] encoded = resp.toString().getBytes();

        // Send to every registered replica, clean up dead ones
        List<OutputStream> dead = new ArrayList<>();
        for (OutputStream replicaOut : replicaStreams) {
            try {
                replicaOut.write(encoded);
                replicaOut.flush();
            } catch (IOException e) {
                System.out.println("[Master] Replica stream dead, removing.");
                dead.add(replicaOut);
            }
        }
        replicaStreams.removeAll(dead);
        System.out.println("[Master] Propagated '" + tokens.get(0).toUpperCase() + "' to " + replicaStreams.size() + " replica(s)");
    }

    private static String executeCommand(List<String> tokens) {
        String command = tokens.get(0).toUpperCase();

        switch (command) {
            case "PING" -> {
                System.out.println("[" + Thread.currentThread().getName() + "] Handled PING -> +PONG");
                return "+PONG\r\n";
            }
            case "INFO" -> {
                // Replication info returned as a RESP Bulk String
                StringBuilder info = new StringBuilder();
                info.append("role:").append(role).append("\r\n");
                if (role.equals("master")) {
                    info.append("master_replid:").append(masterReplId).append("\r\n");
                    info.append("master_repl_offset:").append(masterReplOffset).append("\r\n");
                }
                String infoStr = info.toString();
                System.out.println("[" + Thread.currentThread().getName() + "] Handled INFO replication -> role:" + role);
                return "$" + infoStr.length() + "\r\n" + infoStr + "\r\n";
            }
            case "ECHO" -> {
                String message = tokens.get(1);
                System.out.println("[" + Thread.currentThread().getName() + "] Handled ECHO -> " + message);
                return "$" + message.length() + "\r\n" + message + "\r\n";
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
                String ttlLog = (expiresAt != null) ? " (expires in " + (expiresAt - System.currentTimeMillis()) + "ms)" : "";
                System.out.println("[" + Thread.currentThread().getName() + "] Handled SET " + key + " = " + value + ttlLog);
                // Propagate SET command to all replicas if we are master
                if (role.equals("master")) {
                    propagateToReplicas(tokens);
                }
                return "+OK\r\n";
            }
            case "GET" -> {
                String key = tokens.get(1);
                ValueEntry entry = database.get(key);

                // Passive / Lazy expiration
                if (entry != null && entry.isExpired()) {
                    database.remove(key);
                    entry = null;
                }

                if (entry != null) {
                    String val = entry.value();
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled GET " + key + " -> " + val);
                    return "$" + val.length() + "\r\n" + val + "\r\n";
                } else {
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled GET " + key + " -> (nil)");
                    return "$-1\r\n";
                }
            }
            case "INCR" -> {
                String key = tokens.get(1);
                ValueEntry entry = database.get(key);
                if (entry != null && entry.isExpired()) {
                    database.remove(key);
                    entry = null;
                }
                long val = 0;
                if (entry != null) {
                    try {
                        val = Long.parseLong(entry.value());
                    } catch (NumberFormatException e) {
                        return "-ERR value is not an integer or out of range\r\n";
                    }
                }
                val++;
                database.put(key, new ValueEntry(String.valueOf(val), (entry != null) ? entry.expiresAt() : null));
                System.out.println("[" + Thread.currentThread().getName() + "] Handled INCR " + key + " -> " + val);
                // Propagate INCR to replicas if we are master
                if (role.equals("master")) {
                    propagateToReplicas(tokens);
                }
                return ":" + val + "\r\n";
            }
            case "EXISTS" -> {
                String key = tokens.get(1);
                ValueEntry entry = database.get(key);
                if (entry != null && entry.isExpired()) {
                    database.remove(key);
                    entry = null;
                }
                int exists = (entry != null || listDatabase.containsKey(key) || hashDatabase.containsKey(key)) ? 1 : 0;
                System.out.println("[" + Thread.currentThread().getName() + "] Handled EXISTS " + key + " -> " + exists);
                return ":" + exists + "\r\n";
            }
            case "DEL" -> {
                int deletedCount = 0;
                for (int i = 1; i < tokens.size(); i++) {
                    String key = tokens.get(i);
                    ValueEntry removed = database.remove(key);
                    if (removed != null && !removed.isExpired()) {
                        deletedCount++;
                    }
                    if (listDatabase.remove(key) != null) {
                        deletedCount++;
                    }
                    if (hashDatabase.remove(key) != null) {
                        deletedCount++;
                    }
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Handled DEL -> deleted " + deletedCount + " key(s)");
                if (role.equals("master")) {
                    propagateToReplicas(tokens);
                }
                return ":" + deletedCount + "\r\n";
            }
            case "KEYS" -> {
                List<String> activeKeys = new ArrayList<>();
                for (Map.Entry<String, ValueEntry> e : database.entrySet()) {
                    if (e.getValue().isExpired()) {
                        database.remove(e.getKey());
                    } else {
                        activeKeys.add(e.getKey());
                    }
                }
                // Add keys from listDatabase and hashDatabase
                activeKeys.addAll(listDatabase.keySet());
                activeKeys.addAll(hashDatabase.keySet());

                StringBuilder resp = new StringBuilder();
                resp.append("*").append(activeKeys.size()).append("\r\n");
                for (String k : activeKeys) {
                    resp.append("$").append(k.length()).append("\r\n").append(k).append("\r\n");
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Handled KEYS -> returned " + activeKeys.size() + " key(s)");
                return resp.toString();
            }
            case "HSET" -> {
                String key = tokens.get(1);
                Map<String, String> hash = hashDatabase.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
                int newFields = 0;
                for (int i = 2; i + 1 < tokens.size(); i += 2) {
                    String field = tokens.get(i);
                    String value = tokens.get(i + 1);
                    if (hash.put(field, value) == null) {
                        newFields++;
                    }
                }
                if (role.equals("master")) {
                    propagateToReplicas(tokens);
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Handled HSET " + key + " -> " + newFields + " field(s) added");
                return ":" + newFields + "\r\n";
            }
            case "HGET" -> {
                String key = tokens.get(1);
                String field = tokens.get(2);
                Map<String, String> hash = hashDatabase.get(key);
                String val = (hash != null) ? hash.get(field) : null;
                if (val != null) {
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled HGET " + key + " " + field + " -> " + val);
                    return "$" + val.length() + "\r\n" + val + "\r\n";
                } else {
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled HGET " + key + " " + field + " -> (nil)");
                    return "$-1\r\n";
                }
            }
            case "HGETALL" -> {
                String key = tokens.get(1);
                Map<String, String> hash = hashDatabase.get(key);
                if (hash == null || hash.isEmpty()) {
                    return "*0\r\n";
                }
                StringBuilder resp = new StringBuilder();
                resp.append("*").append(hash.size() * 2).append("\r\n");
                for (Map.Entry<String, String> entry : hash.entrySet()) {
                    resp.append("$").append(entry.getKey().length()).append("\r\n").append(entry.getKey()).append("\r\n");
                    resp.append("$").append(entry.getValue().length()).append("\r\n").append(entry.getValue()).append("\r\n");
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Handled HGETALL " + key + " -> " + hash.size() + " field(s)");
                return resp.toString();
            }
            case "LPUSH" -> {
                String key = tokens.get(1);
                Deque<String> deque = listDatabase.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>());
                for (int i = 2; i < tokens.size(); i++) {
                    deque.addFirst(tokens.get(i));
                }
                if (role.equals("master")) {
                    propagateToReplicas(tokens);
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Handled LPUSH " + key + " -> length " + deque.size());
                return ":" + deque.size() + "\r\n";
            }
            case "RPUSH" -> {
                String key = tokens.get(1);
                Deque<String> deque = listDatabase.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>());
                for (int i = 2; i < tokens.size(); i++) {
                    deque.addLast(tokens.get(i));
                }
                if (role.equals("master")) {
                    propagateToReplicas(tokens);
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Handled RPUSH " + key + " -> length " + deque.size());
                return ":" + deque.size() + "\r\n";
            }
            case "LRANGE" -> {
                String key = tokens.get(1);
                int start = Integer.parseInt(tokens.get(2));
                int stop = Integer.parseInt(tokens.get(3));
                Deque<String> deque = listDatabase.get(key);

                if (deque == null || deque.isEmpty()) {
                    return "*0\r\n";
                }

                List<String> list = new ArrayList<>(deque);
                int size = list.size();

                // Convert negative indices
                if (start < 0) start = size + start;
                if (stop < 0) stop = size + stop;

                // Clamp to list bounds
                if (start < 0) start = 0;
                if (stop >= size) stop = size - 1;

                if (start > stop || start >= size) {
                    return "*0\r\n";
                }

                int count = stop - start + 1;
                StringBuilder resp = new StringBuilder();
                resp.append("*").append(count).append("\r\n");
                for (int i = start; i <= stop; i++) {
                    String item = list.get(i);
                    resp.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Handled LRANGE " + key + " " + start + " " + stop + " -> " + count + " item(s)");
                return resp.toString();
            }
            case "CONFIG" -> {
                if (tokens.size() >= 3 && tokens.get(1).equalsIgnoreCase("GET")) {
                    String param = tokens.get(2).toLowerCase();
                    String val = "";
                    if (param.equals("dir")) {
                        val = dir;
                    } else if (param.equals("dbfilename")) {
                        val = dbfilename;
                    }
                    System.out.println("[" + Thread.currentThread().getName() + "] Handled CONFIG GET " + param + " -> " + val);
                    return "*2\r\n" +
                            "$" + param.length() + "\r\n" + param + "\r\n" +
                            "$" + val.length() + "\r\n" + val + "\r\n";
                } else {
                    return "-ERR unsupported CONFIG subcommand\r\n";
                }
            }
            case "REPLCONF" -> {
                System.out.println("[" + Thread.currentThread().getName() + "] Handled REPLCONF -> +OK");
                return "+OK\r\n";
            }
            case "PSYNC" -> {
                System.out.println("[" + Thread.currentThread().getName() + "] Handled PSYNC -> +FULLRESYNC " + masterReplId + " 0");
                return "+FULLRESYNC " + masterReplId + " 0\r\n";
            }
            default -> {
                return "-ERR unknown command '" + command + "'\r\n";
            }
        }
    }
}
