package edu.ucla.cs.cdsc.benchmarks;

import edu.ucla.cs.cdsc.pipeline.*;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Created by Peter on 10/10/2017.
 */
public final class SWPipeline extends Pipeline {
    private static final Logger logger = Logger.getLogger(SWPipeline.class.getName());
    private static final SWPipeline singleton = new SWPipeline(1 << 21);
    private HashMap<Integer, SWUnpackObject> unpackObjects;
    private AtomicInteger numPackThreads;
    private int TILE_SIZE;

    public SWPipeline(int TILE_SIZE) {
        this.numPackThreads = new AtomicInteger(0);
        this.TILE_SIZE = TILE_SIZE;
        this.unpackObjects = new HashMap<>(256);
        for (int i=0; i<256; i++) {
            this.unpackObjects.put(i, new SWUnpackObject());
        }
    }

    public static SWPipeline getSingleton() {
        return singleton;
    }

    public HashMap<Integer, SWUnpackObject> getUnpackObjects() {
        return unpackObjects;
    }


    @Override
    public SendObject pack(PackObject obj) {
        return null;
    }

    @Override
    public void send(SendObject obj, Socket socket) {
        try {
            //Socket socket = new Socket();
            //SocketAddress address = new InetSocketAddress("127.0.0.1", 6070);
            /*
            while (true) {
                try {
                    socket.connect(address);
                    break;
                } catch (Exception e) {
                    logger.warning("Connection failed, try it again");
                }
            }
            */
            byte[] data = ((SWSendObject) obj).getData();
            //logger.info("[Pipeline] Sending data with length " + data.length + ": " + (new String(data)).substring(0, 64));
            //BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
            //out.write(data, 0, TILE_SIZE);
            byte[] dataLength = new byte[4];
            int batchSize = data.length;
            dataLength[3] = (byte) ((batchSize >> 24) & 0xff);
            dataLength[2] = (byte) ((batchSize >> 16) & 0xff);
            dataLength[1] = (byte) ((batchSize >> 8) & 0xff);
            dataLength[0] = (byte) ((batchSize >> 0) & 0xff);
            socket.getOutputStream().write(dataLength);
            socket.getOutputStream().write(data);
            //socket.close();
        } catch (Exception e) {
            logger.severe("[Send] Caught exception: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public RecvObject receive(InputStream in) {
        try {
            byte[] dataSizeBytes = new byte[4];
            in.read(dataSizeBytes, 0, 4);
            int overallSize = ByteBuffer.wrap(dataSizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            logger.info("Overall size is " + overallSize);
            byte[] data = new byte[overallSize];
            //BufferedInputStream in = new BufferedInputStream(incoming.getInputStream());
            //in.read(data, 0, TILE_SIZE);
            int n;
            //InputStream in = incoming.getInputStream();
            int offset = 0, length = overallSize;
            while ((n = in.read(data, offset, length)) > 0) {
                if (n == length) break;
                offset += n;
                length -= n;
            }
            //in.read(data);
            logger.info("Received data with length " + offset);
            //incoming.close();
            return new SWRecvObject(data);
        } catch (Exception e) {
            logger.severe("[Recv] Caught exception: " + e);
            e.printStackTrace();
            return new SWRecvObject(null);
        }
    }

    @Override
    public UnpackObject unpack(RecvObject obj) {
        return null;
    }

    public int acquireThreadID() {
        int threadID = (numPackThreads.getAndIncrement()) & 0xff;
        return threadID;
    }

    public void releaseThreadID(int id) {
        unpackObjects.remove(id);
    }

    @Override
    public Object execute(Object input) {
        //logger.info("[Pipeline] pipeline initialization begins");
        long overallStartTime = System.nanoTime();

        Runnable sender = () -> {
            try (Socket socket = new Socket("127.0.0.1", 6070)) {
                boolean done = false;
                while (!done) {
                    SWSendObject obj;
                    while ((obj = (SWSendObject) getSendQueue().poll()) == null) ;
                    //logger.info("[Pipeline] obtained a valid input from the send queue");
                    if (obj.getData() == null) {
                        //done = true;
                    } else {
                        //logger.info("[Pipeline] the size of the batch is " + obj.getData().length);
                        send(obj, socket);
                    }
                }
            } catch (Exception e) {
                logger.severe("[Sender] Caught exception: " + e);
                e.printStackTrace();
            }
        };

        Runnable receiver = () -> {
            try (ServerSocket server = new ServerSocket(9520)) {
                //server.setReuseAddress(true);
                //server.bind(new InetSocketAddress(9520));

                Socket incoming = server.accept();
                logger.info("Java receiver is connected by the C scatter");
                InputStream in = incoming.getInputStream();

                boolean done = false;
                while (!done) {
                    //logger.info("numJobs = " + numJobs.get() + ", numPendingJobs = " + numPendingJobs.get());
                    SWRecvObject curObj = (SWRecvObject) receive(in);
                    if (curObj.getData() == null) done = true;
                    else {
                        int curThreadID = ((int) curObj.getData()[3]) & 0xff;
                        AtomicReference<byte[]> curReference = unpackObjects.get(curThreadID).getData();
                        while (curReference.get() != null) ;
                        curReference.set(curObj.getData());
                    }
                }
            } catch (Exception e) {
                logger.severe("[Receiver] Caught exception: " + e);
                e.printStackTrace();
            }
        };

        Thread sendThread = new Thread(sender);
        sendThread.start();
        Thread recvThread = new Thread(receiver);
        recvThread.start();

        return null;

        /*
        try {
            sendThread.join();
            recvThread.join();
        } catch (Exception e) {
            logger.severe("Caught exception: " + e);
            e.printStackTrace();
        }

        long overallTime = System.nanoTime() - overallStartTime;
        System.out.println("[Overall] " + overallTime / 1.0e9);
        return null;
        */
    }
}

