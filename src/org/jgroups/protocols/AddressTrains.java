package org.jgroups.protocols;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.jgroups.Address;
import org.jgroups.util.Util;

public class AddressTrains implements Address {
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return String.valueOf(address);
	}

	private int address = 0;

	public int getAddress() {
		return this.address;
	}

	public AddressTrains() {

	}

	public AddressTrains(int addr) {
		this.address = addr;
	}

	@Override
	public void writeTo(DataOutput out) throws Exception {
		// Too bad, there is no Util.writeInt :-(
		Util.writeObject(Integer.valueOf(address), out);
		// TODO Auto-generated method stub

	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		address = ((Integer) Util.readObject(in)).intValue();
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(Address o) {
		return this.getAddress() - ((AddressTrains) o).getAddress();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

}