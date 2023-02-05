package org.ptg.quarkdb;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class Relation implements Externalizable {
	int idx;
	EWAHCompressedBitmap outs = new EWAHCompressedBitmap();

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(idx);
		outs.writeExternal(out);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		idx = in.readInt();
		outs.readExternal(in);
	}

	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}

	public EWAHCompressedBitmap getOuts() {
		return outs;
	}

	public void setOuts(EWAHCompressedBitmap outs) {
		this.outs = outs;
	}

	public void addRel(int idx) {
		outs.set(idx);
	}

	public List<Integer> getOutRelations() {
		return outs.toList();
	}

}
