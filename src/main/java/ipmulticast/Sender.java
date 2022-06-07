package ipmulticast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * Created by meslahik on 16.11.17.
 */
public class Sender {
    public void sendTo(Object object, String host, int port) {
        try {
            InetAddress group = InetAddress.getByName(host);
            byte[] buff = new byte[1000];
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(byteArray);
            oos.flush();
            oos.writeObject(object);
            oos.flush();
            buff = byteArray.toByteArray();
            DatagramPacket packet = new DatagramPacket(buff, buff.length, group, port);
            MulticastSocket socket = new MulticastSocket();
            socket.send(packet);
            socket.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
