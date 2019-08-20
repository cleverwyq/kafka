import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import java.nio.channels.SocketChannel;

public class ApiClientMock {
    public static void main(String[] args) {
        try {
            SocketChannel sc = SocketChannel.open();
            //sc.configureBlocking(false);
            sc.connect(new InetSocketAddress("127.0.0.1", 9092));
            boolean c = sc.finishConnect();
            System.out.println(c);

            byte[] b = new byte[]{0x00, 0x00, 0x00, 0x0b, 0x00, 0x12, 0x00, 0x02,0x00, 0x00,0x00, 0x00, 0x00, 0x01, 0x61};
            ByteBuffer buffer = ByteBuffer.allocate(15);
            buffer.put(b);
            buffer.rewind();
            int w = sc.write(buffer);
            System.out.println(w);
            System.out.println(buffer);
            ByteBuffer dst = ByteBuffer.allocate(512);
            if (sc.isOpen())
                System.out.println("open");
            int read = sc.read(dst);
            System.out.println(read);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}
