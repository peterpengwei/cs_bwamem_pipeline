package edu.ucla.cs.cdsc.pipeline;

import jdk.nashorn.internal.ir.Block;
import org.jctools.queues.*;
import org.jctools.queues.SpscLinkedQueue;

import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Peter on 10/10/2017.
 */
public abstract class Pipeline {
    private static final int PACK_QUEUE_SIZE = 32;
    private static final int SEND_QUEUE_SIZE = 32;
    private static final int RECV_QUEUE_SIZE = 32;
    private static final int UNPACK_QUEUE_SIZE = 32;

    private SpscLinkedQueue<PackObject> packQueue = new SpscLinkedQueue<>();
    //private SpscLinkedQueue<SendObject> sendQueue = new SpscLinkedQueue<>();
    private MpscArrayQueue<SendObject> sendQueue = new MpscArrayQueue<>(SEND_QUEUE_SIZE);
    private SpscLinkedQueue<RecvObject> recvQueue = new SpscLinkedQueue<>();
    private SpscLinkedQueue<UnpackObject> unpackQueue = new SpscLinkedQueue<>();

    public SpscLinkedQueue<PackObject> getPackQueue() {
        return packQueue;
    }

    public MpscArrayQueue<SendObject> getSendQueue() {
        return sendQueue;
    }

    public SpscLinkedQueue<RecvObject> getRecvQueue() {
        return recvQueue;
    }

    public SpscLinkedQueue<UnpackObject> getUnpackQueue() {
        return unpackQueue;
    }

    public abstract SendObject pack(PackObject obj);

    public abstract void send(SendObject obj, Socket socket);

    public abstract RecvObject receive(ServerSocket server);

    public abstract UnpackObject unpack(RecvObject obj);

    public abstract Object execute(Object input);
}
