import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.TreeMap;

public class Client {

    class NodeProxy {
        Socket socket;
        BufferedReader in;
        PrintWriter out;

        NodeProxy(int port) throws IOException {
            this.socket = new Socket("localhost", port);
            InputStream inputStream = this.socket.getInputStream();
            OutputStream outputStream = this.socket.getOutputStream();
            this.in = new BufferedReader(new InputStreamReader(inputStream));
            this.out = new PrintWriter(outputStream);
        }
    }

    Map<Integer, NodeProxy> nodes;

    Client() {
        nodes = new TreeMap<>();
    }

    void addNode(int port) throws IOException {
        NodeProxy node = new NodeProxy(port);
        String _port = String.valueOf(port);
        nodes.put(MyHash.hash(_port), node);
        System.out.println("connected to DataNode@" + port);
    }

    void removeNode(int port) {

    }

    String put(String key, String value) throws IOException {
        if (nodes.size() == 0) return "没有 DataNode";
        int hashKey = MyHash.hash(key);
        int i = hashKey;
        boolean found = false;
        while (i < MyHash.MAX_HASH) {
            if (nodes.containsKey(i)) {
                found = true;
                break;
            }
            i++;
            if (i == MyHash.MAX_HASH){
                i = 0;
            } else if (i == hashKey){
                break;
            }
        }
        if (found) {
            // 找到存储节点
            NodeProxy node = nodes.get(i);
            node.out.println(String.format("put %s %s", key, value));
            node.out.flush();
            return node.in.readLine();
        } else {
            return "找不到 DataNode";
        }
    }

    String get(String key) {

    }

    Map<String, String> getAll() {

    }

    void close() throws IOException {
        for(NodeProxy node: nodes.values()) {
            node.socket.close();
        }
    }

    // store 1 2 3
    public static void main(String[] args) {
        if (args.length <= 2) {
            System.out.println("用法：Client <DataNode端口号> ...");
            return;
        }
        Client client = new Client(); // TODO:
    }
}
