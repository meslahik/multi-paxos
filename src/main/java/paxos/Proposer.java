package paxos;

import config.ConfigFileReader;
import ipmulticast.Receiver;
import ipmulticast.Sender;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by meslahik on 15.11.17.
 */
public class Proposer {
    // receive value from client
    // forward it to the leader
    // leader election
    // interactions with acceptors

    Logger logger;
    ConfigFileReader config;
    int proposerId;
    final int NUM_OF_ACCEPTORS = 3;
    final int MAJORITY = NUM_OF_ACCEPTORS/2 + 1;
    final int MAX_NUM_POROPOSERS = 100;
    int leaderId;
    boolean isLeader = false;

    int highestPaxosInstance = 0;
    Map<Integer, PaxosInstance> paxosInstances = new ConcurrentHashMap<>();

    // I suppose the main process polling the acceptorQueue would wait when it is empty, the thread can insert elements when it receives
    LinkedBlockingQueue<Message> acceptorQueue = new LinkedBlockingQueue<>();
//    Queue<Message> acceptorQueue = new LinkedList<>();
    LinkedBlockingQueue<Integer> clientQueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Message> proposerQueue = new LinkedBlockingQueue<>();
    Set<Message> requestSet = ConcurrentHashMap.newKeySet();

    public Proposer (int id, String configFile) {
        logger = LoggerFactory.getLogger(Proposer.class);
        proposerId = id;
        config = new ConfigFileReader(configFile);

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Iterator it = requestSet.iterator();
                if (it.hasNext()) {
                    Message msg = (Message) it.next();
                    while(it.hasNext()) {
                        Message msg2 = (Message)it.next();
                        if (msg2.getPaxosInstanceId() < msg.getPaxosInstanceId())
                            msg = msg2;
                    }
                    logger.debug("send msg {} to acceptors again as not received enough responses", msg);
                    sendToAcceptors(msg);
                }
//                Message msg = Collections.min(requestSet, Comparator.comparing(Message::getPaxosInstanceId));
            }
        }, 200, 100);

        Thread clientMessageProcessorThread = new Thread(new ClientMessageProcessor());
        clientMessageProcessorThread.start();

        Thread reliableDeliverer = new Thread(new ReliableDeliverer());
        reliableDeliverer.start();

        Thread leaderElector = new Thread(new LeaderElectionProcessor());
        leaderElector.start();
    }

    void sendToProposers(Message msg) {
        Sender sender = new Sender();
        sender.sendTo(msg, config.getProposersAddr().getIP(), config.getProposersAddr().getPort());
    }

    void sendToAcceptors(Message msg) {
        Sender sender = new Sender();
        sender.sendTo(msg, config.getAcceptorsAddr().getIP(), config.getAcceptorsAddr().getPort());
    }

    void sendToLearners(Message msg) {
        Sender sender = new Sender();
        sender.sendTo(msg, config.getLearnersAddr().getIP(), config.getLearnersAddr().getPort());
    }

    void removeFromReqestSet(Message msg) {
        final int paxosInstanceId = msg.getPaxosInstanceId();
        final MessageType type = msg.getType();
        Message msgInRequestSet = requestSet.stream().filter(x -> x.getPaxosInstanceId() == paxosInstanceId &&
                ((x.getType() == MessageType.ONEA && type == MessageType.ONEB) || (x.getType() == MessageType.TWOA && type == MessageType.TWOB)))
                .findAny().orElse(null);
        if (msgInRequestSet != null) {
            logger.debug("remove {} from request set", msgInRequestSet);
            requestSet.remove(msgInRequestSet);
        }
    }

    void execute() {
        Message msg = null;
        while (true) {
            try {
                msg = acceptorQueue.take();
            } catch (Exception ex) {
                System.out.println("Something went wrong reading the acceptorQueue of messages");
                ex.printStackTrace();
            }

            if (!isLeader)
                continue;
            if (msg.getPaxosInstanceId() > highestPaxosInstance) // TODO: do all proposers contains this?
                continue;

            PaxosInstance instance = paxosInstances.get(msg.getPaxosInstanceId());
            if (msg.getHighestRound() > instance.waitingRound) {
                removeFromReqestSet(msg);
                Message newMsg = instance.prepareONEAMsg();
                newMsg.setSenderId(proposerId);
                requestSet.add(newMsg);
                logger.debug("send msg {} to acceptors", newMsg);
                sendToAcceptors(newMsg);
                continue;
            }

            boolean isAdded = instance.addAcceptorMessage(msg);
            if (!isAdded)
                continue;
            if (!instance.hasMajorityReached())
                continue;
            removeFromReqestSet(msg);

            logger.debug("processing message {}", msg);
            switch (msg.getType()) {
                case ONEB:
                    logger.debug("Majority of ONB messages reached for paxos instance {}", instance.paxosInstanceId);
                    // if received any value from acceptors, set value with max round number as the value of the instance
                    if (instance.messagesReceived.stream().anyMatch(Message::getHasValue)) {
                        clientQueue.add(instance.value);
                        int value = instance.messagesReceived.stream().filter(Message::getHasValue).findFirst().get().getValue();
                        instance.setValue(value);
                    }

                    // send
                    Message newmsgTWOA = instance.prepareTWOAMsg();
                    newmsgTWOA.setSenderId(proposerId);
                    requestSet.add(newmsgTWOA);
                    logger.debug("send the message {} to acceptors ...", newmsgTWOA);
                    sendToAcceptors(newmsgTWOA);

//                        if (instance.hasMajorityAccepted()) {
//                            Message newmsg = instance.prepareTWOAMsg();
//                            sendToAcceptors(newmsg);
//                        }
//                        else {
//                            Message notAcceptedMessage = instance.notAcceptedMessage();
//                            int hrnd = notAcceptedMessage.getHighestRound();
//                            int val = notAcceptedMessage.getValue();
//                            if (hrnd > instance.waitingRound && val == -1) {
//                                instance.setWaitingRound(hrnd+1);
//                                Message newmsg = instance.prepareONEAMsg();
//                                sendToAcceptors(newmsg);
//                            }
//                            else { // 1-majority decided before (go for learner) 2- only this one (go for 2A) => so has to go for 2A
//                                instance.setWaitingRound(hrnd+1); // hrnd or hrnd+1 ??
//                                instance.setValue(val);
//                                Message newmsg = instance.prepareTWOAMsg();
//                                sendToAcceptors(newmsg);
//                            }
//                        }
////                        if (msg.value != -1) {
////                            paxosInstances.put(msg.paxosInstance, msg);
////
////                            msg.type = MessageType.THREE; // ???
////                            sendToLearners(msg.value);
////                        }
////                        else if (msgStored.round == msg.highestRound) {
////                            msgStored.type = MessageType.TWOA;
////                            msgStored.value = instance.value;
////                            sendToAcceptors(msgStored);
////                        }
////                        else if (msgStored.round < msg.highestRound) {
////                            msgStored.round = msg.highestRound + 1;
////                            sendToAcceptors(msgStored);
////                        }
                    break;
                case TWOB:
                    logger.debug("Majority of TWOB messages received for paxos instance {}", instance.paxosInstanceId);
                    // I suppose it is the only case can happen
                    instance.hasDecided = true;
                    Message newmsgTHREE = new Message(MessageType.THREE, instance.paxosInstanceId, instance.value);
                    logger.debug("value={} decided", instance.value);
                    logger.debug("send msg {} to learner...", newmsgTHREE);
                    sendToLearners(newmsgTHREE);

//                        if (msgStored.round >= msg.highestRound) {
//                            msg.type = MessageType.THREE;
//                            sendToLearners(msg.value);
//                        }
//                        else if (msgStored.round < msg.highestRound) {
//
//                        }

//                        Message msgStored = paxosInstances.get(msg.paxosInstance);
//                        msgStored.type = MessageType.THREE;
//                        msgStored.round = msg.round;
//                        msgStored.value = msg.value;
//                        sendToLearners(msg.value);

                    break;
            }
        }
    }

    class PaxosInstance {
        private int paxosInstanceId;
        private boolean isWaiting;
        MessageType waitingType;
        private AtomicInteger waitingId = new AtomicInteger(0);
        private int waitingRound;
        private int value;
        private boolean hasDecided = false;
        Set<Integer> acceptorsId = new HashSet<>();
        Map<Integer, Integer> receivedValues = new HashMap<>();
        int numOfTryToPropose = 0;

//        int numOfResponses = 0;
        private ArrayList<Message> messagesReceived = new ArrayList<>();

        public PaxosInstance(int value) {
            paxosInstanceId = ++highestPaxosInstance;
            setValue(value);
        }

        void setValue(int value) {
            this.value = value;
        }
        void setWaitingRound(int rnd) {
            waitingRound = rnd;
        }

        boolean addAcceptorMessage(Message msg) {
            if (!isWaiting) {
                logger.debug("ignore msg {}. this instance is not waiting for any response", msg);
                return false;
            }
            if (waitingType != msg.getType()) {
                logger.debug("ignore msg {}. type of the msg {} does not match with what this instance is waiting: {}", msg, msg.getType(), waitingType);
                return false;
            }
            if (msg.getWaitingId() != waitingId.get()) {
                logger.debug("ignore msg {}. current waiting id is {}", msg, isWaiting);
                return false;
            }
            if (acceptorsId.contains(msg.getSenderId())) {
                logger.debug("ignore msg {}. instance currently contains acceptor {} id in its list of acceptors response {}", msg, msg.getSenderId(), acceptorsId);
                return false;
            }

            logger.debug("add msg {} to instance message received list", msg);
            messagesReceived.add(msg);
            acceptorsId.add(msg.getSenderId());
            if (msg.getType() == MessageType.ONEB) {
                if (acceptorsId.size() == MAJORITY)
                    isWaiting = false;
            } else if (msg.getType() == MessageType.TWOB) {
                if (receivedValues.containsKey(msg.getValue()))
                    receivedValues.put(msg.getValue(), receivedValues.get(msg.getValue()) + 1);
                else
                    receivedValues.put(msg.getValue(), 1);
                logger.debug("value {} with count={} in received values", msg.getValue(), receivedValues.get(msg.getValue()));
                boolean majority = receivedValues.keySet().stream().anyMatch(x -> receivedValues.get(x) == MAJORITY);
                if (majority)
                    isWaiting = false;
            }
            return true;
        }

//        Message prepareONEAMsg(int hrnd) {
//            messagesReceived.clear();
//            isWaiting = true;
//
//            Message msg = new Message(paxosInstanceId, ++waitingId, MessageType.ONEA, hrnd); //
////            msg.paxosInstance = ;
////            msg.type = MessageType.ONEA;
////            msg.round = 0;
//            return msg;
//        }

        Message prepareONEAMsg() {
            messagesReceived.clear();
            acceptorsId.clear();
            receivedValues.clear();
            isWaiting = true;
            waitingType = MessageType.ONEB;
            waitingRound = proposerId + numOfTryToPropose*MAX_NUM_POROPOSERS;
            numOfTryToPropose++;

            Message msg = new Message(paxosInstanceId, waitingId.incrementAndGet(), MessageType.ONEA, waitingRound);
//            msg.paxosInstance = ;
//            msg.type = MessageType.ONEA;
//            msg.round = 0;
            return msg;
        }

        Message prepareTWOAMsg() {
            messagesReceived.clear();
            acceptorsId.clear();
            receivedValues.clear();
            isWaiting = true;
            waitingType = MessageType.TWOB;

            Message msg = new Message(paxosInstanceId, waitingId.incrementAndGet(), MessageType.TWOA, waitingRound);
            msg.setValue(value);
            return msg;
        }

//        int maxHighestRoundOfAcceptors() {
//            return messagesReceived.stream().max(Comparator.comparing(Message::getHighestRound)).get().getHighestRound();
//        }

//        int maxRoundOfAcceptors() {
//            return messagesReceived.stream().max(Comparator.comparing(Message::getRound)).get().getRound();
//        }

//        int maxValueOfAcceptors() {
//            Set<Message> values = new HashSet<>();
//            for(Message msg: messagesReceived)
//                if (msg.getHasValue())
//                    values.add(msg);
//            return values.stream().max(Comparator.comparing(Message::getRound)).get().getValue();
//        }

        boolean hasMajorityReached() {
            return !isWaiting;
        }

        //        boolean hasMajorityAccepted() {
//            boolean res = true;
//            for (Message msg: messagesReceived)
//                if (!msg.hasAccepted()) {
//                    res = false;
//                    break;
//                }
//            return res;
//        }

//        Message notAcceptedMessage() {
//            ArrayList<Message> notAcceptedList = new ArrayList<>();
////            for (Message msg: messagesReceived)
////                if (!msg.hasAccepted)
////                    notAcceptedList.add(msg);
//            messagesReceived.stream().filter(x -> !x.hasAccepted()).forEach(notAcceptedList::add);
//
//            Message highestPriorityMsg = notAcceptedList.stream().filter(x -> x.getType() == MessageType.TWOB).max(Comparator.comparing(Message::getHighestRound)).orElse(null);
//            if (highestPriorityMsg != null)
//                return highestPriorityMsg;
//
//            highestPriorityMsg = notAcceptedList.stream().filter(x -> x.getType() == MessageType.ONEB).max(Comparator.comparing(Message::getHighestRound)).orElse(null);
//            return highestPriorityMsg;
//        }
    }

    class LeaderElectionProcessor implements Runnable{
        Timer pulseTimer = new Timer();

        void notLeaderAnymore() {
            isLeader = false;
            pulseTimer.cancel();
        }
        void changeLeaderId(int id) {
            leaderId = id;
            logger.debug("[changed] Leader id: {}", leaderId);
        }

        void becomeLeader() {
            changeLeaderId(proposerId);
            isLeader = true;
            pulseTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    Message newmsg = new Message();
                    newmsg.setType(MessageType.LEADER);
                    newmsg.setLeaderId(proposerId);
                    sendToProposers(newmsg);
                }
            }, 200, 200);
        }

        Message takeMessage() {
            ExecutorService service = Executors.newSingleThreadExecutor();
            Callable c = new Callable<Message>() {
                @Override
                public Message call() {
                    try {
                        Message msg = proposerQueue.take();
                        return msg;
                    } catch (InterruptedException ex) {
                        // nothing
                    }
                    return null;
                }
            };
            Future<Message> f = service.submit(c);
            try {
                return f.get(1, TimeUnit.SECONDS);
            } catch (InterruptedException|ExecutionException ex) {
                ex.printStackTrace();
            } catch (TimeoutException ex) {
                logger.debug("Too Long!");
                f.cancel(true);
            } finally {
                service.shutdown();
            }
            return null;
        }

        @Override
        public void run() {
            while(true) {
                Message msg = takeMessage();
                if (msg == null)
                    becomeLeader();
                else if (msg.getLeaderId() < leaderId || leaderId == 0) {
                    if (leaderId == proposerId)
                        notLeaderAnymore();
                    changeLeaderId(msg.getLeaderId());
                }
                else {
//                    logger.debug("Leader id: {}", leaderId);
                }
            }
        }
    }

    class ClientMessageProcessor implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    int value = clientQueue.take();
                    if (!isLeader)
                        continue;
                    logger.debug("client value {} received", value);
                    PaxosInstance instance = new PaxosInstance(value);
                    paxosInstances.put(instance.paxosInstanceId, instance);
                    logger.debug("paxos instance {} has been created", instance.paxosInstanceId);
                    Message newMsg = instance.prepareONEAMsg();
                    newMsg.setSenderId(proposerId);
                    requestSet.add(newMsg);
                    logger.debug("send msg {} to acceptors", newMsg);
                    sendToAcceptors(newMsg);
                } catch (InterruptedException ex) {
                    System.out.println("Something went wrong reading the clientQueue");
                    ex.printStackTrace();
                }
            }
        }
    }

    class ReliableDeliverer implements Runnable {
        Receiver receiver;

        public ReliableDeliverer() {
            String host = config.getProposersAddr().getIP();
            int port = config.getProposersAddr().getPort();
            receiver = new Receiver(host, port);
        }

//        public boolean rdeliver(Message msg) {
//            acceptorQueue.add(msg);
//            return true;
//        }

        @Override
        public void run() {
            // What does it do? it waits for the messages and insert them in the acceptorQueue
            // How it waits?
            // How others find this class to send message to it?
            // maybe IP Multicast? subscribe and wait for the message?

            while(true) {
                try {
                    Message msg = (Message) receiver.receive();
                    if (msg.getType() == MessageType.CLIENT)
                        clientQueue.put(msg.getValue());
                    else if (msg.getType() == MessageType.LEADER)
                        proposerQueue.put(msg);
                    else
                        acceptorQueue.put(msg);
                } catch (InterruptedException ex) {
                    System.out.println("Error in putting in the queue");
                    ex.printStackTrace();
                }
            }
        }
    }

    static void Usage() {
        System.out.println("Usage: java Proposer <id> <config-file>");
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            Usage();
            return;
        }

        int id = Integer.parseInt(args[0]);
        String configFile = args[1];

        Proposer proposer = new Proposer(id, configFile);
        proposer.execute();
    }
}
