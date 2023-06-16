import java.io.*;
import java.net.Socket;
import java.util.*;

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

//    // 将节点按照哈希值排序
//    void sortMap(){
//        List<Map.Entry< Integer,NodeProxy>> entries = new ArrayList<>(nodes.entrySet());
//
//        Collections.sort(entries, new Comparator<Map.Entry<Integer,NodeProxy>>() {
//            @Override
//            public int compare(Map.Entry<Integer,NodeProxy> entry1, Map.Entry<Integer,NodeProxy> entry2) {
//                return entry1.getKey().compareTo(entry2.getKey());
//            }
//        });
//    }


    // 增加节点 TODO: 数据迁移
    void addNode(int port) throws IOException {
        NodeProxy node = new NodeProxy(port);
        String _port = String.valueOf(port);
        nodes.put(MyHash.hash(_port), node);
        System.out.println("connected to DataNode@" + port);

        // 找出源数据所在节点
        Integer nowHash= MyHash.hash(_port);
        Integer oldHash=MyHash.MAX_HASH;
        NodeProxy oldNode = null;

        for (Map.Entry<Integer,NodeProxy> entry : nodes.entrySet()) {
            Integer key = entry.getKey();
            if (key > nowHash){
                if (key < oldHash){
                    oldNode = entry.getValue();
                    oldHash=key;
                }
            }
        }

        Map<String,String> data=oldNode.getAll();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
//            System.out.println(key + ": " + value);
            Integer keyHash=MyHash.hash(key);

            if (keyHash > nowHash) oldNode.put(key,value);
            else node.put(key,value);
        }
    }

    // TODO: 移除节点
    void removeNode(int port) {

        String _port=String.valueOf(port);
        NodeProxy oldNode = nodes.get(MyHash.hash(_port));
        Integer oldHash= MyHash.hash(_port);

        //找出新节点
        Integer   newHash=MyHash.MAX_HASH;
        NodeProxy newNode = null;
        for (Map.Entry<Integer,NodeProxy> entry : nodes.entrySet()) {
            Integer key = entry.getKey();
            if (key > oldHash){
                if (key < newHash){
                    newNode = entry.getValue();
                    newHash=key;
                }
            }
        }

        Map<String,String> data;
        try {
            data = oldNode.getAll();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
//            System.out.println(key + ": " + value);

            newNode.put(key,value);
        }

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
