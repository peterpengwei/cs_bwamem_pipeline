package edu.ucla.cs.cdsc.benchmarks;

import edu.ucla.cs.cdsc.pipeline.SendObject;

/**
 * Created by Peter on 10/16/2017.
 */
public class SWSendObject extends SendObject {
    private byte[] data;

    public SWSendObject(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
