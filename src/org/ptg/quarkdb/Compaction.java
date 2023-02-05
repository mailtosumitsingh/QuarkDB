package org.ptg.quarkdb;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Compaction implements Externalizable {
	int[] arr = new int[0];

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(arr.length);
		for (int i = 0; i < arr.length; i++) {
			out.writeInt(arr[i]);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int len = in.readInt();
		arr = new int[len];
		for (int i = 0; i < len; i++) {
			arr[i] = in.readInt();
		}
	}

	public int[] getArr() {
		return arr;
	}

	public void setArr(int[] arr) {
		this.arr = arr;
	}

}
