import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class Client {

    // 数据节点代理类
    static class NodeProxy {
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

        void put(String key, String value) {
            out.println(String.format("put %s %s", key, value));
            out.flush();
        }

        String get(String key) throws IOException {
            out.println(String.format("get %s", key));
            out.flush();
            return in.readLine();
        }

        // 取出节点全部键值对 用于移除节点操作
        Map<String, String> getAll() throws IOException {
            Map<String, String> data = new HashMap<>();
            out.println("get all");
            out.flush();
            String line;
            while ((line = in.readLine()) != null) {
                String[] args = line.split(" ");
                data.put(args[0], args[1]);
            }
            return data;
        }

        void close() throws IOException {
            in.close();
            out.close();
            socket.close();
        }
    }

    // 节点表
    Map<Integer, NodeProxy> nodes;

    Client() {
        nodes = new TreeMap<>();
    }

    // 增加节点 TODO: 数据迁移
    void addNode(int port) throws IOException {
        NodeProxy node = new NodeProxy(port);
        String _port = String.valueOf(port);
        nodes.put(MyHash.hash(_port), node);
        System.out.println("connected to DataNode@" + port);
    }

    // TODO: 移除节点
    void removeNode(int port) {
    }

    // 根据键值寻找所在节点
    NodeProxy locateNode(String key) {
        if (nodes.size() == 0) {
            System.out.println("没有 DataNode");
            return null;
        }
        int hashKey = MyHash.hash(key);
        int i = hashKey;
        while (i < MyHash.MAX_HASH) {
            if (nodes.containsKey(i)) {
                break;
            }
            i++;
            if (i == MyHash.MAX_HASH) {
                i = 0;
            } else if (i == hashKey) {
                break;
            }
        }
        return nodes.get(i);
    }

    void put(String key, String value) {
        NodeProxy node = locateNode(key);
        node.put(key, value);
    }

    String get(String key) throws IOException {
        NodeProxy node = locateNode(key);
        return node.get(key);
    }

    /*
     * 命令
     * put key value
     * get key 返回 value
     * add <port>
     * remove <port>
     * quit
     */
    void repl() throws NumberFormatException, IOException {
        Scanner sc = new Scanner(System.in);
        String line;
        REPL:
        while (true) {
            line = sc.nextLine();
            String[] args = line.split(" ");
            if (line.equals("quit")) break;
            switch (args[0]) {
                case "quit":
                    break REPL;
                case "put":
                    put(args[1], args[2]);
                    break;
                case "get":
                    line = get(args[1]);
                    System.out.println(line);
                    break;
                case "add": {
                    int _port = Integer.parseInt(args[1]);
                    addNode(_port);
                    break;
                }
                case "remove": {
                    int _port = Integer.parseInt(args[1]);
                    removeNode(_port);
                    break;
                }
                default:
                    System.out.println("无效命令");
                    break;
            }
        }
    }

    void close() throws IOException {
        for (NodeProxy node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }

    /*
     * 命令
     * Client 8000
     */
    public static void main(String[] args) throws NumberFormatException, IOException {
        if (args.length == 0) {
            System.out.println("用法：Client <DataNode端口号> ...");
            return;
        }
        Client client = new Client();
        for (String port : args) {
            int _port = Integer.parseInt(port);
            client.addNode(_port);
        }
        client.repl();
        client.close();
        System.out.println("close");
    }
}
