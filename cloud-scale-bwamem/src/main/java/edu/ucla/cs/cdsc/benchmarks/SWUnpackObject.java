package edu.ucla.cs.cdsc.benchmarks;

import edu.ucla.cs.cdsc.pipeline.UnpackObject;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Peter on 10/16/2017.
 */
public class SWUnpackObject extends UnpackObject {

    public SWUnpackObject() {
        this.data = null;
    }

    public AtomicReference<byte[]> getData() {
        return data;
    }

    public void setData(AtomicReference<byte[]> data) {
        this.data = data;
    }

    private AtomicReference<byte[]> data;
}
