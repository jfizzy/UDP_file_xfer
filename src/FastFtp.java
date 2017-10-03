
import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

/**
 * FastFtp Class
 *
 * FastFtp implements a basic FTP application based on UDP data transmission.
 * The main method is send() which takes a file name as input argument and send
 * the file to the specified destination host.
 *
 */
public class FastFtp {

    private final int windowSize;
    private final int rtoTimer;

    private Socket connSocket;
    private DataInputStream in;
    private DataOutputStream out;

    private DatagramSocket udpSocket;

    private InetAddress localAddr;
    private int localPort;

    private InetAddress remoteAddr;
    private int remotePort;

    private TxQueue txQueue;

    private Timer timer;
    
    private int segCount;

    /**
     * Constructor to initialize the program
     *
     * @param windowSize	Size of the window for Go-Back_N (in segments)
     * @param rtoTimer	The time-out interval for the retransmission timer (in
     * milli-seconds)
     */
    public FastFtp(int windowSize, int rtoTimer) {
        this.windowSize = windowSize;
        this.rtoTimer = rtoTimer;
    }

    /**
     * Sends the specified file to the specified destination host: 1. send file
     * name and receiver server confirmation over TCP 2. send file segment by
     * segment over UDP 3. send end of transmission over tcp 3. clean up
     *
     * @param serverName	Name of the remote server
     * @param serverPort	Port number of the remote server
     * @param fileName	Name of the file to be transferred to the remote server
     */
    public void send(String serverName, int serverPort, String fileName) {
        try {
            // *** check if file exists, otherwise exit
            if (fileExistsAndReadable(fileName)) {
                int port = tcpConf(serverName, serverPort, fileName);
                if (port > 0) {
                    udpTransmit(fileName);
                } else {
                    throw new Exception("unable to complete TCP Handshake");
                }
            } else {
                throw new Exception("Not a valid file");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Established TCP connection to the destination host: 1. Establishes
     * connection 2. sends filename to server 3. interprets server response byte
     * 4. returns port used after closing TCP connection
     *
     * @param serverName
     * @param serverPort
     * @param fileName
     * @return foreign port number for UDP transmission, -1 for error
     */
    private int tcpConf(String serverName, int serverPort, String fileName) {
        try {
            connSocket = new Socket(serverName, serverPort);
            in = new DataInputStream(connSocket.getInputStream());
            out = new DataOutputStream(connSocket.getOutputStream());

            out.writeUTF(fileName); // write filename to server

            byte resp = in.readByte();
            System.out.println("Response from server: [" + resp + "]");

            localPort = connSocket.getLocalPort();
            localAddr = connSocket.getLocalAddress();

            remotePort = connSocket.getPort();
            remoteAddr = connSocket.getInetAddress();

            return remotePort;

        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Checks if the file exists and can be read
     *
     * @param path
     * @return is both there and readable
     */
    private boolean fileExistsAndReadable(String path) {
        File file = new File(System.getProperty("user.dir") + '/' + path);
        if (file.exists()) {
            return file.canRead();
        } else {
            return false;
        }
    }

    private void udpTransmit(String fileName) throws SocketException, UnknownHostException {
        udpSocket = new DatagramSocket(localPort, localAddr); // udp socket with specified server port
        udpSocket.setSoTimeout(rtoTimer*10);
        byte[] fileContents = fileContents(fileName);
        System.out.println("Acquired file contents");

        txQueue = new TxQueue(this.windowSize);

        // split file into segments
        ArrayList<Segment> segments = splitSegments(Segment.MAX_PAYLOAD_SIZE, fileContents);
        segCount = segments.size();
        System.out.println("*************");
        System.out.println("Split file contents into segments");

        //begin listening for acks
        ReceiverThread rt = new ReceiverThread(this);
        rt.start();
        System.out.println("Receiver thread started");

        try {
            System.out.println("File has " + segments.size() + " segments");
            for (Segment segment : segments) {
                synchronized (txQueue) {
                    while (txQueue.isFull()) {
                        Thread.yield();
                    }
                }
                System.out.println(">>>>Sending segment: " + segment.getSeqNum());
                processSend(segment);
            }
            rt.join(); // wait for the ack receiver thread
            udpSocket.close();
            out.writeByte(0); // signify end of session to server
            in.close();
            out.close();
            connSocket.close();
        } catch (Exception e) {
            e.getMessage();
        }
        
        
    }

    public synchronized void processSend(Segment seg) {
        try {
            DatagramPacket sendPacket = new DatagramPacket(seg.getBytes(), seg.getBytes().length, remoteAddr, remotePort);
            // send seg to the UDP socket
            udpSocket.send(sendPacket);
            // add seg to the transmission queue txQueue
            txQueue.add(seg);
            // if txQueue.size() == 1, start the timer
            if (txQueue.size() == 1) {
                timer = new Timer(true);
                timer.schedule(new TimeoutHandler(this), rtoTimer);
            }
        } catch (IOException | InterruptedException e) {
            e.getMessage();
        }
    }

    public synchronized void processACK(Segment seg) {
        try {
            // if ACK not in the current window, do nothing
            System.out.println("<<<<Received ACK: " + seg.getSeqNum());
            if (seg.getSeqNum() < txQueue.element().getSeqNum() || seg.getSeqNum() > (txQueue.element().getSeqNum() + windowSize)) {
                System.out.println("Invalid ACK");
                //outsize of window - ignore
                System.out.println("Segment: " + seg.getSeqNum() + " is outside of window (seg " + txQueue.element().getSeqNum() + " -> seg " + txQueue.element().getSeqNum() + windowSize + ")");
            } else {
                // cancel the timer
                timer.cancel();
                // while txQueue.element().getSeqNum() < ack.getSeqNum()
                while (txQueue.element().getSeqNum() < seg.getSeqNum()) {
                    txQueue.remove();
                }
                // if not txQueue.isEmpty(), start the timer
                if (!txQueue.isEmpty()) {
                    timer = new Timer(true);
                    timer.schedule(new TimeoutHandler(this), rtoTimer);
                }
            }
        } catch (Exception e) {
            e.getMessage();
        }
    }

    public synchronized void processTimeout() {
        try {
            System.out.println("****Timeout");
            Segment[] segsToTx = txQueue.toArray();
            // get the list of all pending segments by calling txQueue.toArray()
            for (Segment seg : segsToTx) {
                System.out.println(">>>>Re-sending segment: " + seg.getSeqNum());
                DatagramPacket sendPacket = new DatagramPacket(seg.getBytes(), seg.getBytes().length, remoteAddr, remotePort);
                udpSocket.send(sendPacket);
            }
            // go through the list and send all segments to the UDP socket
            if (!txQueue.isEmpty()) {
                timer = new Timer(true);
                timer.schedule(new TimeoutHandler(this), rtoTimer);// start new timer
            }
            // if not txQueue.isEmpty(), start the timer
        } catch (Exception e) {

        }
    }

    private ArrayList<Segment> splitSegments(int chunkSize, byte[] fileContents) {
        System.out.println("Splitting file into segments:");
        System.out.println("*************");
        int len = fileContents.length;
        int counter = 0;
        int lenDivC = len / chunkSize;
        System.out.println("file length (bytes): " + len);
        System.out.println("segment size: " + chunkSize);
        System.out.println("file len / seg size = " + lenDivC);
        ArrayList<Segment> result;
        result = new ArrayList<>();
        if (!(len <= chunkSize)) { // 1 chunk in file of less than chunkSize
            System.out.println("File is at least one segment is size!");
            for (int i = 0; i < lenDivC; i++) {
                result.add(new Segment(i, Arrays.copyOfRange(fileContents, i * chunkSize, ((i * chunkSize) + chunkSize))));
                counter = i;
            }
            if (len % chunkSize == 0) { // length is divisible by chunk size
                return result;
            } else {
                result.add(new Segment(counter + 1, Arrays.copyOfRange(fileContents, (counter * chunkSize) + chunkSize, len)));
                return result;
            }
        } else {
            System.out.println("File is shorter or equal to one segment in size!");
            result.add(new Segment(0, Arrays.copyOf(fileContents, len)));
            return result;
        }
    }

    private byte[] fileContents(String path) {
        try {
            File file = new File(System.getProperty("user.dir") + '/' + path);
            Path p = file.toPath();
            return Files.readAllBytes(p);
        } catch (IOException e) {
            System.out.println("Unable to get file contents");
            return null;
        }
    }

    private class TimeoutHandler extends TimerTask {

        private FastFtp fftp;

        public TimeoutHandler(FastFtp fftp) {
            this.fftp = fftp;
        }

        @Override
        public void run() {
            fftp.processTimeout();
        }
    }

    private class ReceiverThread extends Thread {

        private FastFtp fftp;
        private volatile boolean shutdown;

        public ReceiverThread(FastFtp fftp) {
            this.fftp = fftp;
            shutdown = false;
        }

        @Override
        public void run() {
            System.out.println("ReceiverThread is running");
            int lastACK = 0;
            while (!(lastACK == segCount)) {
                try {
                    byte[] receiveData = new byte[1024];

                    DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

                    udpSocket.receive(receivePacket);
                    Segment s = new Segment(receivePacket);
                    
                    this.fftp.processACK(s); // this never gets reached
                    if(s.getSeqNum() == (lastACK+1))
                        lastACK++;
                } catch (Exception e) {
                    // socket receive timeout
                }
            }
            System.out.println("last valid ACK ["+lastACK+"] = segment count ["+segCount+"]");
            System.out.println("ReceiverThread Done.");

        }

    }

    /**
     * A simple test driver
     *
     */
    public static void main(String[] args) {
        int windowSize = 10; //segments
        int timeout = 100; // milli-seconds

        String serverName = "localhost";
        String fileName = "";
        int serverPort = 0;

        // check for command line arguments
        if (args.length == 3) {
            // either privide 3 paramaters
            serverName = args[0];
            serverPort = Integer.parseInt(args[1]);
            fileName = args[2];
        } else if (args.length == 2) {
            // or just server port and file name
            serverPort = Integer.parseInt(args[0]);
            fileName = args[1];
        } else {
            System.out.println("wrong number of arguments, try again.");
            System.out.println("usage: java FastFtp server port file");
            System.exit(0);
        }

        FastFtp ftp = new FastFtp(windowSize, timeout);

        System.out.printf("sending file \'%s\' to server...\n", fileName);
        ftp.send(serverName, serverPort, fileName);
        System.out.println("file transfer completed.");
    }

}
