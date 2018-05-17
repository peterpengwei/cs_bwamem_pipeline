package edu.ucla.cs.cdsc.benchmarks;

import edu.ucla.cs.cdsc.pipeline.RecvObject;

/**
 * Created by Peter on 10/16/2017.
 */
public class SWRecvObject extends RecvObject {
    private byte[] data;

    public SWRecvObject(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
