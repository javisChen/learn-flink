package learn.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ReceiveServer {

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket( 9091);
            System.out.println("监听成功");
            while (true) {
                System.out.println("等待连接");
                Socket socket = serverSocket.accept();
                System.out.println("接收连接：" + socket);
                InputStream inputStream = socket.getInputStream();
                byte[] bytes = new byte[1024];
                int read = 0;
                System.out.println("接收数据：");
                while ((read = inputStream.read(bytes)) != -1) {
                    System.out.println(new String(bytes, 0, read));
                }
                inputStream.close();
                socket.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
