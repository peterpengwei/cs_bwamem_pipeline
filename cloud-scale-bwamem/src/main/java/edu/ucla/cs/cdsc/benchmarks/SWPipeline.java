package edu.ucla.cs.cdsc.benchmarks;

import edu.ucla.cs.cdsc.pipeline.*;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by Peter on 10/10/2017.
 */
public class SWPipeline extends Pipeline {
    private static final Logger logger = Logger.getLogger(SWPipeline.class.getName());

    public static void setNumPackThreads(AtomicInteger numPackThreads) {
        SWPipeline.numPackThreads = numPackThreads;
    }

    public ArrayList<SWUnpackObject> getUnpackObjects() {
        return unpackObjects;
    }

    public void setUnpackObjects(ArrayList<SWUnpackObject> unpackObjects) {
        this.unpackObjects = unpackObjects;
    }

    private static AtomicInteger numPackThreads;
    private int TILE_SIZE;

    private ArrayList<SWUnpackObject> unpackObjects;

    private AtomicInteger numPendingJobs;
    private int numFPGAJobs;

    public static SWPipeline getSingleton() {
        return singleton;
    }

    private static SWPipeline singleton;

    static {
        singleton = new SWPipeline(1 << 20);
    }

    public SWPipeline(int TILE_SIZE) {
        numPendingJobs = new AtomicInteger(0);
        numFPGAJobs = 0;
        this.TILE_SIZE = TILE_SIZE;
    }

    @Override
    public SendObject pack(PackObject obj) {
        return null;
    }

    @Override
    public void send(SendObject obj) {
        try {
            Socket socket = new Socket();
            SocketAddress address = new InetSocketAddress("127.0.0.1", 6070);
            while (true) {
                try {
                    socket.connect(address);
                    break;
                } catch (Exception e) {
                    logger.warning("Connection failed, try it again");
                }
            }
            byte[] data = ((SWSendObject) obj).getData();
            //logger.info("Sending data with length " + data.length + ": " + (new String(data)).substring(0, 64));
            //BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
            //out.write(data, 0, TILE_SIZE);
            socket.getOutputStream().write(data);
            socket.close();
        } catch (Exception e) {
            logger.severe("Caught exception: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public RecvObject receive(ServerSocket server) {
        try (Socket incoming = server.accept()) {
            byte[] data = new byte[TILE_SIZE];
            //BufferedInputStream in = new BufferedInputStream(incoming.getInputStream());
            //in.read(data, 0, TILE_SIZE);
            int n;
            InputStream in = incoming.getInputStream();
            int offset = 0, length = TILE_SIZE;
            while ((n = in.read(data, offset, length)) > 0) {
                if (n == length) break;
                offset += n;
                length -= n;
            }
            //in.read(data);
            //logger.info("Received data with length " + data.length + ": " + (new String(data)).substring(0, 64));
            incoming.close();
            return new SWRecvObject(data);
        } catch (Exception e) {
            logger.severe("Caught exceptino: " + e);
            e.printStackTrace();
            return new SWRecvObject(null);
        }
    }

    @Override
    public UnpackObject unpack(RecvObject obj) {
        return null;
    }

    public int acquireThreadID() {
        int threadID = numPackThreads.getAndIncrement();
        unpackObjects.add(new SWUnpackObject());
        return threadID;
    }

    @Override
    public Object execute(Object input) {
        long overallStartTime = System.nanoTime();

        Runnable sender = () -> {
            try {
                boolean done = false;
                while (!done) {
                    SWSendObject obj;
                    while ((obj = (SWSendObject) SWPipeline.getSendQueue().poll()) == null) ;
                    if (obj.getData() == null) {
                        //done = true;
                    } else {
                        numFPGAJobs++;
                        send(obj);
                    }
                }
            } catch (Exception e) {
                logger.severe("Caught exception: " + e);
                e.printStackTrace();
            }
        };

        Runnable receiver = () -> {
            try (ServerSocket server = new ServerSocket()) {
                server.setReuseAddress(true);
                server.bind(new InetSocketAddress(9520));

                boolean done = false;
                while (!done) {
                    //logger.info("numJobs = " + numJobs.get() + ", numPendingJobs = " + numPendingJobs.get());
                    if (numPendingJobs.get() > 0) {
                        SWRecvObject curObj = (SWRecvObject) receive(server);
                        numPendingJobs.getAndDecrement();
                        if (curObj.getData() == null) done = true;
                        else {
                            while (SWPipeline.getRecvQueue().offer(curObj) == false) ;
                        }
                    }
                }
            } catch (Exception e) {
                logger.severe("Caught exception: " + e);
                e.printStackTrace();
            }
        };

        Runnable unpacker = () -> {
            try {
                boolean done = false;
                while (!done) {
                    SWRecvObject curObj;
                    while ((curObj = (SWRecvObject) SWPipeline.getRecvQueue().poll()) == null) ;
                    if (curObj.getData() == null) done = true;
                    else {
                        int curThreadID = curObj.getData()[0];
                        while (unpackObjects.get(curThreadID).getData().compareAndSet(null, curObj.getData())) ;
                    }
                }
            } catch (Exception e) {
                logger.severe("Caught exception: " + e);
            }
        };

        Thread sendThread = new Thread(sender);
        sendThread.start();
        Thread recvThread = new Thread(receiver);
        recvThread.start();
        Thread unpackThread = new Thread(unpacker);
        unpackThread.start();
        //Thread mergeThread = new Thread(merger);
        //mergeThread.start();

        return null;

        /*
        try {
            sendThread.join();
            recvThread.join();
            unpackThread.join();
            //mergeThread.join();
        } catch (Exception e) {
            logger.severe("Caught exception: " + e);
            e.printStackTrace();
        }

        long overallTime = System.nanoTime() - overallStartTime;
        System.out.println("[Overall] " + overallTime / 1.0e9);
        System.out.println("[FPGA Jobs] " + numFPGAJobs);
        //return stringBuilder.toString();
        System.out.println();
        return null;
        */

    }

    public int getTILE_SIZE() {
        return TILE_SIZE;
    }

    public void setTILE_SIZE(int TILE_SIZE) {
        this.TILE_SIZE = TILE_SIZE;
    }

    public AtomicInteger getNumPendingJobs() {
        return numPendingJobs;
    }

    public void setNumPendingJobs(AtomicInteger numPendingJobs) {
        this.numPendingJobs = numPendingJobs;
    }
}

