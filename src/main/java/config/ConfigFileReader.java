package config;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by meslahik on 16.11.17.
 */
public class ConfigFileReader {
    Address clientsAddr;
    Address proposersAddr;
    Address acceptorsAddr;
    Address learnersAddr;

    public ConfigFileReader(String configFile) {
        try {
            String line;
            FileReader reader = new FileReader(configFile);
            BufferedReader bReader = new BufferedReader(reader);
            while((line = bReader.readLine()) != null) {
                String[] segLine = line.split("\\s+");

                switch (segLine[0]) {
                    case "clients":
                        clientsAddr = new Address(segLine[1], Integer.parseInt(segLine[2]));
                        break;
                    case "proposers":
                        proposersAddr = new Address(segLine[1], Integer.parseInt(segLine[2]));
                        break;
                    case "acceptors":
                        acceptorsAddr = new Address(segLine[1], Integer.parseInt(segLine[2]));
                        break;
                    case "learners":
                        learnersAddr = new Address(segLine[1], Integer.parseInt(segLine[2]));
                        break;
                    default:
                        System.out.println("Error in file!");
                }
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public Address getClientsAddr() {
        return clientsAddr;
    }

    public Address getProposersAddr() {
        return proposersAddr;
    }

    public Address getAcceptorsAddr() {
        return acceptorsAddr;
    }

    public Address getLearnersAddr() {
        return learnersAddr;
    }

    public class Address {
        String IP;
        int port;

        public Address(String IP, int port) {
            this.IP = IP;
            this.port = port;
        }

        public String getIP() { return IP; }
        public int getPort() { return port; }
    }

}
