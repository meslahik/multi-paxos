package paxos;

import config.ConfigFileReader;
import ipmulticast.Sender;
import monitor.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Semaphore;

/**
 * Created by meslahik on 14.12.17.
 */
public class ClientLearner extends Learner {
    Logger logger = LoggerFactory.getLogger(ClientLearner.class);
    private ConfigFileReader configFileReader;
    private Sender sender = new Sender();
    int outstanding;
    private Semaphore sendPermits;
    Monitor monitor;
    Map<Integer, Long> valueStartTime = new HashMap<>();

    public ClientLearner(int id, String configFile, int outstanding, int duration, int warmup) {
        super(id, configFile, true);
        this.configFileReader = new ConfigFileReader(configFile);
        this.outstanding = outstanding;
        sendPermits = new Semaphore(outstanding);
        monitor = new Monitor(duration, warmup);

        Thread batchRun = new Thread(new BatchSender());
        batchRun.start();

        execute();
    }

    void sendToProposer(int value) {
        Message msg = new Message(MessageType.CLIENT, value);
        logger.debug("send msg {} to proposers", msg);
        sender.sendTo(msg, configFileReader.getProposersAddr().getIP(), configFileReader.getProposersAddr().getPort());
    }

    @Override
    void hasAdded(int value) {
        long end = System.currentTimeMillis();
        long start = valueStartTime.get((value));
        monitor.addLatency(end - start);
        monitor.incrementTp();
        valueStartTime.remove(value);
        addPermit();
    }

    void getPermit() {
        try {
            sendPermits.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    void addPermit() {
        if (sendPermits != null)
            sendPermits.release();
    }

    static void Usage() {
        System.out.println("Usage: java ClientLearner <id> <config-file> <outstanding> <duration> <warmup>");
    }

    class BatchSender implements Runnable {
        @Override
        public void run() {
            while (true) {
                getPermit();
                Random random = new Random();
                int rand = random.nextInt();
                if (rand < 0)
                    rand = rand * (-1);
                valueStartTime.put(rand, System.currentTimeMillis());
                sendToProposer(rand);
                if (monitor.isFinished)
                    break;
            }
        }
    }

    public static void main(String[] args) {

        if (args.length != 5) {
            Usage();
            return;
        }
        int id = Integer.parseInt(args[0]);
        String configFile = args[1];
        int outstanding = Integer.parseInt(args[2]);
        int duration = Integer.parseInt(args[3]);
        int warmup = Integer.parseInt(args[4]);

        ClientLearner client = new ClientLearner(id, configFile, outstanding, duration, warmup);
    }
}
