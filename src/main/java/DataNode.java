import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/*
* 接收消息
* put key value
* get key
* quit
* 返回消息
* ok
* value
*/

public class DataNode {
    int port;
    Map<String, String> data;

    DataNode(int port) throws IOException {
        this.port = port;
        this.data = new HashMap<>();

        // 创建监听对象
        ServerSocket listenSocket = new ServerSocket(port);
        System.out.println("DataNode @" + port);
        // 从连接队列中取出一个记录并创建新的通信socket
        Socket clientSocket = listenSocket.accept();
        System.out.println("连接到 Client");

        InputStream inputStream = clientSocket.getInputStream();
        OutputStream outputStream = clientSocket.getOutputStream();
        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
        PrintWriter out = new PrintWriter(outputStream);

        String line;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
            String[] args = line.split(" ");
            if(args.length >= 3 && args[0].equals("put")) {
                data.put(args[1], args[2]);
                line = "ok";
            } else if (args.length >= 2 && args[0].equals("get")) {
                line = data.get(args[1]);
            } else if (args[0].equals("quit")) break;
            System.out.println(line);
            out.println(line);
            out.flush();
        }

        clientSocket.close();
        listenSocket.close();
    }

    void run() {

    }

    void close() {

    }

    public static void main(String[] args) {
        if (args.length <= 1) {
            System.out.println("用法：DataNode <端口号>");
            return;
        }
        int port;
        try{
            port = Integer.parseInt(args[0]);
            DataNode dn = new DataNode(port);
        } catch (NumberFormatException e) {
            System.out.println("非法输入");
        } catch (IOException e) {
            System.out.println("IOException");
        }
    }
}
