package edu.ucla.cs.cdsc.benchmarks;

import edu.ucla.cs.cdsc.pipeline.UnpackObject;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Peter on 10/16/2017.
 */

public class SWUnpackObject extends UnpackObject {

    public SWUnpackObject() {
        this.data = new AtomicReference<>(null);
    }

    public AtomicReference<byte[]> getData() {
        return data;
    }

    public void setData(AtomicReference<byte[]> data) {
        this.data = data;
    }

    private AtomicReference<byte[]> data;
}


/*
public class SWUnpackObject extends UnpackObject {
    private boolean writeFlag;
    private boolean readFlag;
    private volatile byte[] pingReference;
    private volatile byte[] pongReference;

    public SWUnpackObject() {
        pingReference = null;
        pongReference = null;
        writeFlag = false;
        readFlag = false;
    }

    public void write(byte[] data) {
        if (writeFlag == false) {
            //while (pingReference != null) ;
            pingReference = data;
            writeFlag = true;
        }
        else {
            //while (pongReference != null);
            pongReference = data;
            writeFlag = false;
        }
    }

    public byte[] read() {
        byte[] data = null;
        if (readFlag == false) {
            while (pingReference == null) ;
            data = pingReference;
            pingReference = null;
            readFlag = true;
        }
        else {
            while (pongReference == null) ;
            data = pongReference;
            pongReference = null;
            readFlag = false;
        }
        return data;
    }
}
*/