package paxos;

import config.ConfigFileReader;
import ipmulticast.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * Created by meslahik on 22.11.17.
 */
public class Client {
    Logger logger = LoggerFactory.getLogger(Client.class);
    private ConfigFileReader configFileReader;
    private Sender sender = new Sender();

    public Client(int id, String configFile) {
        configFileReader = new ConfigFileReader(configFile);
    }

    void sendToProposer(int value) {
        Message msg = new Message(MessageType.CLIENT, value);
        logger.debug("send msg {} to proposers", msg);
        sender.sendTo(msg, configFileReader.getProposersAddr().getIP(), configFileReader.getProposersAddr().getPort());
    }

    static void Usage() {
        System.out.println("Usage: java Client <id> <config-file>");
    }

    static void runInteractive(Client client) {
        Scanner scanner = new Scanner(System.in);
        String input;
        while(scanner.hasNextLine()) {
            input = scanner.nextLine();
            int value = Integer.parseInt(input);
            client.sendToProposer(value);
        }
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            Usage();
            return;
        }
        int id = Integer.parseInt(args[0]);
        String configFile = args[1];

        Client client = new Client(id, configFile);
        runInteractive(client);
    }
}
