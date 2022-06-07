package ipmulticast;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;

/**
 * Created by meslahik on 16.11.17.
 */
public class Receiver {
    MulticastSocket socket;

    public Receiver(String host, int port) {
        try {
            InetAddress group = InetAddress.getByName(host);
            socket = new MulticastSocket(port);
            socket.joinGroup(group);
        } catch (UnknownHostException ex) {
            socket = null;
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public Object receive() {
        try {
            byte[] buff = new byte[1000];
            DatagramPacket packet = new DatagramPacket(buff, buff.length);
            socket.receive(packet); // Does it wait for the incoming messages? yes

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buff));
            return ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
