import java.io.IOException;
import java.net.Socket;

public class Step1Test {
    public static void main(String[] args) throws Exception {
        // Run the server in a background thread
        Thread serverThread = new Thread(() -> Main.main(new String[0]));
        serverThread.start();

        // Give the server a moment to start
        Thread.sleep(500);

        // Connect as a client
        try (Socket socket = new Socket("127.0.0.1", 6379)) {
            System.out.println("TEST SUCCESS: Successfully connected to Redis server on port 6379!");
            System.out.println("Is connected: " + socket.isConnected());
        } catch (IOException e) {
            System.err.println("TEST FAILED: Could not connect to Redis server: " + e.getMessage());
            System.exit(1);
        }

        serverThread.join(2000);
        System.out.println("TEST COMPLETE: Server handled connection and closed cleanly.");
    }
}
