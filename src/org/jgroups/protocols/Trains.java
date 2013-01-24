package org.jgroups.protocols;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import trains.CallbackCircuitChange;
import trains.CallbackUtoDeliver;
import trains.CircuitView;
import trains.Interface;

import org.jgroups.util.Util;

/**
 * This class is in charge of communication with trains protcole implemented in
 * C.
 * 
 * @author Tiezhen WANG
 * @since 3.2.5
 */
@Experimental
@MBean(description = "Train protocol wrapper")
public class Trains extends Protocol {

	/*
	 * ----------------------------------------- Properties
	 * --------------------------------------------------
	 */
	@Property(description = "input file name")
	String inputFile = "test.txt";

	@Property(description = "The number of trains on the circuit (default 1)")
	int trainsNumber = 1;

	@Property(description = "The wagons length during the execution (default 32KB)")
	int wagonLength = 32000;

	@Property(description = "The max number of waits (default 10)")
	int waitNb = 10;

	@Property(description = "The default time to wait, in microsec (default 2)")
	int waitTime = 2;

	/*
	 * --------------------------------------------- Fields
	 * ------------------------------------------------------
	 */
	protected volatile boolean running = true;
	protected volatile Address local_addr, next;
	protected int view_size = 0;
	protected Executor default_pool = null;
	protected Executor oob_pool = null;

	private Interface trin = null;
	private Trains that;
	private Semaphore stateTransferSemaphore = null;
	private String cluster = null;

	/*
	 * --------------------------------------------- Methods
	 * ------------------------------------------------------
	 */
	public List<Integer> providedUpServices() {
		List<Integer> retval = new ArrayList<Integer>(5);
		retval.add(Event.GET_DIGEST);
		retval.add(Event.OVERWRITE_DIGEST);

		retval.add(Event.MSG);
		retval.add(Event.VIEW_CHANGE);
		retval.add(Event.TMP_VIEW);
		retval.add(Event.SET_LOCAL_ADDRESS);
		retval.add(Event.REMOVE_ADDRESS);
		retval.add(Event.CONNECT_USE_FLUSH);
		retval.add(Event.DISCONNECT);
		return retval;
	}

	public void resetStats() {
		super.resetStats();
	}

	public void init() throws Exception {
		stateTransferSemaphore = new Semaphore(1, true);

		System.out.println("Trains init");

		/*
		 * Load JNI lib //
		 */

		/*
		 * Create TrainsJniProxy
		 */

		myCallbackCircuitChange mycallbackCircuit = myCallbackCircuitChange
				.getInstance();
		mycallbackCircuit.setTrainsProtocolInstance(this);

		myCallbackUtoDeliver mycallbackUto = myCallbackUtoDeliver.getInstance();
		mycallbackUto.setTrainsProtocolInstance(this);

		System.out.println("** Load interface");
		trin = Interface.trainsInterface();

		/*
		 * thread pool (why?)
		 */
		// default_pool = getTransport().getDefaultThreadPool();
		// oob_pool = getTransport().getOOBThreadPool();
	}

	public void start() throws Exception {
		System.out.println("Trains start");

		super.start();
		running = true;
	}

	public void stop() {
		System.out.println("Trains stop");

		int exitcode = trin.JtrTerminate();
		if (exitcode < 0) {
			System.out.println("JtrInit terminate failed.");
			return;
		}

		super.stop();
		running = false;

		// need to add this line otherwise it won't really exit
		System.exit(0);
	}

	public Object down(final Event evt) {
		// System.out.println("Trains down");
		int exitcode = 0;

		switch (evt.getType()) {
		case Event.CONNECT:
		case Event.CONNECT_USE_FLUSH:
		case Event.CONNECT_WITH_STATE_TRANSFER:
		case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
			this.cluster = (String) evt.getArg();

			// System.out.println("Trains down connect type = " +
			// evt.getType());
			System.out.println("** trInit");
			exitcode = trin.JtrInit(trainsNumber, wagonLength, waitNb,
					waitTime, myCallbackCircuitChange.class.getName(),
					myCallbackUtoDeliver.class.getName());

			if (exitcode < 0) {
				System.out.println("JtrInit failed.");
				return null;
			}
			break;

		case Event.MSG:

			Message msg = (Message) evt.getArg();
			// msg.setSrc(local_addr); // sender for trains is not important
			msg.setSrc(new AddressTrains(Interface.JgetMyAddress()));

			msg.putHeader(this.id, new TrainHeader(this.cluster));

			System.out.println("Trains down message " + msg);

			trains.Message msgTrains;
			try {
				byte[] payload = Util.objectToByteBuffer(msg);
				msgTrains = trains.Message.messageFromPayload(payload);

				trin.Jnewmsg(msgTrains.getPayload().length,
						msgTrains.getPayload());

				exitcode = trin.JutoBroadcast(msgTrains);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (exitcode < 0) {
				System.out.println("JutoBroadcast failed.");
				break;
			}

			break;

		// case Event.VIEW_CHANGE:
		// System.out.println("Trains down view_change");
		//
		// handleView((View) evt.getArg());
		// break;

		// case Event.TMP_VIEW:
		// System.out.println("Trains down tmp_view");
		//
		// view_size = ((View) evt.getArg()).size();
		// break;

		case Event.SET_LOCAL_ADDRESS:
			System.out.println("Trains down set_local_address");

			local_addr = (Address) evt.getArg();
			break;

		case Event.REMOVE_ADDRESS:
			System.out.println("Trains down remove_address");
			break;

		case Event.DISCONNECT:
			System.out.println("Trains down disconnect");
			break;

		case Event.CONFIG:
			System.out.println("Trains down config");
			break;

		case Event.CLOSE_BARRIER:
			stateTransferSemaphore.release();
			System.out.println("Trains close barrier");
			break;

		case Event.SUSPEND_STABLE:
			System.out.println("Trains suspend stable");
			break;

		case Event.OPEN_BARRIER:
			try {
				stateTransferSemaphore.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Trains open barrier");
			break;

		case Event.RESUME_STABLE:
			System.out.println("Trains resume stable");
			break;

		default:
			System.out.println("Trains down unknown type = "
					+ Event.type2String(evt.getType()));
			break;
		}
		// return down_prot.down(evt);
		return null;
	}

	//
	// // TODO: add sender
	// public void myCallbackUtoDeliver(Message msg) {
	// Event evt = new Event(Event.MSG, msg);
	// this.up(evt);
	// }
	//
	// public void myCallbackCircuitChange(View view) {
	// Event evt = new Event(Event.VIEW_CHANGE, view);
	// this.up(evt);
	// }

	//
	// public class myCallbackCircuitChange implements CallbackCircuitChange{
	// public void run(){
	//
	// }
	// }

	public Object up(Event evt) {
		// System.out.println("Trains up");

		switch (evt.getType()) {
		case Event.MSG:
			System.out.println("Trains up message");
			break;

		case Event.CONFIG:
			System.out.println("Trains up config");
			break;

		case Event.GET_LOCAL_ADDRESS:
			// Channel will handle this event
			System.out.println("Trains up get_local_address");
			break;

		case Event.VIEW_CHANGE:
			System.out.println("Trains up view_change");

			handleView((View) evt.getArg());
			break;

		default:
			System.out.println("Trains up unknown type = "
					+ Event.type2String(evt.getType()));

			break;
		}
		return up_prot.up(evt);
	}

	protected void handleView(View view) {
		System.out.println("Trains handleView");

		view_size = view.size();
		Address tmp = Util.pickNext(view.getMembers(), local_addr);
		if (tmp != null && !tmp.equals(local_addr)) {
			next = tmp;
			if (log.isDebugEnabled())
				log.debug("next=" + next);
		}
	}

	private static final class myCallbackCircuitChange implements
			CallbackCircuitChange {

		private static final myCallbackCircuitChange CIRCUITCHANGE = new myCallbackCircuitChange();

		private Trains prot = null;

		private myCallbackCircuitChange() {
			// Nothing to do
			System.out.println("circuit change constuctor.");
		}

		public static myCallbackCircuitChange getInstance() {
			return CIRCUITCHANGE;
		}

		public void setTrainsProtocolInstance(Trains prot) {
			this.prot = prot;
			System.out.println("prot = " + prot);
		}

		@Override
		public void run(CircuitView cv) {
			try {
				this.prot.stateTransferSemaphore.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.prot.stateTransferSemaphore.release();
			// Printing the circuit modification

			int addrMy = Interface.JgetMyAddress();

			// use element 0 as a key indicator
			// TODO: still have problem of matching local_addr!
			AddressTrains creator = null;
			for (int i = 0; i < cv.getMemb(); i++) {
				int addrI = cv.getMembersAddress(i);
				if (addrMy != addrI) {
					creator = new AddressTrains(addrI);
					break;
				}
			}
			// Printing the new/departed participant
			if (cv.getJoined() != 0) {
				// creator = new AddressTrains(cv.getJoined());
				System.out.println(Integer.toString(cv.getJoined())
						+ " has arrived.");
			} else {
				// creator = new AddressTrains(cv.getDeparted());
				System.out.println(Integer.toString(cv.getDeparted())
						+ " is gone.");
			}
			//
			ArrayList<Address> members = new ArrayList<Address>();
			// members.add(creator);
			// members.add(new AddressTrains(1));
			for (int i = 0; i < cv.getMemb(); i++) {
				int rank = i;
				System.out.print("rank = " + rank);
				int addr = cv.getMembersAddress(rank);
				System.out.println(", addr = " + addr);
				if (addrMy != addr) {
					members.add(new AddressTrains(addr));
				}
			}
			members.add(new AddressTrains(addrMy));
			// Need cv -> members as an array
			View view = new View(creator, 0, members);
			Event evt = new Event(Event.VIEW_CHANGE, view);
			prot.up(evt);

			// Printing the current number of members
			System.out
					.println("Currently " + cv.getMemb() + " in the circuit.");
			System.out.println("prot = " + prot);
		}
	}

	private static final class myCallbackUtoDeliver implements
			CallbackUtoDeliver {

		private static final myCallbackUtoDeliver UTODELIVER = new myCallbackUtoDeliver();

		private Trains prot = null;

		public myCallbackUtoDeliver() {
			// Nothing to do
			System.out.println("message delivery constuctor. ");
		}

		public static myCallbackUtoDeliver getInstance() {
			return UTODELIVER;
		}

		public void setTrainsProtocolInstance(Trains prot) {
			this.prot = prot;
			System.out.println("prot = " + prot);
		}

		@Override
		public void run(int sender, trains.Message msgTrains) {
			try {
				this.prot.stateTransferSemaphore.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.prot.stateTransferSemaphore.release();

			Message msg = null;
			try {
				msg = (Message) Util.objectFromByteBuffer(msgTrains
						.getPayload());
				// msg.setSrc(new AddressTrains(sender));
				// System.out.println("received from = " + msg.getSrc());
				System.out.println("received msg = " + msg);
				// System.out
				// .println("The content is " + (String) msg.getObject());
				// msg = new Message(null, null, str);
				// System.out.println("type is " + msgTrains.getPayload()[0]);
				// System.out.println("prot = " + prot);

				String clusterName = ((TrainHeader) msg.getHeader(prot.id))
						.getClusterName();

				// System.out.println("msg cluster = " + clusterName
				// + ", prot.clusterName = " + prot.cluster);

				if (prot.cluster.equals(clusterName)) {
					if (msg.getDest() == null) {
						// send normal message
						Event evt = new Event(Event.MSG, msg);
						prot.up(evt);
					}
					if (msg.getDest() != null) {
						// e.g. state transfer msg
						if (Interface.JgetMyAddress() != ((AddressTrains) msg
								.getSrc()).getAddress()) {
							// msg is not sent by me
							Event evt = new Event(Event.MSG, msg);
							prot.up(evt);
						}
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static class TrainHeader extends Header {
		private String clusterName;

		public TrainHeader() {
		}

		public TrainHeader(String clusterName) {
			this.clusterName = clusterName;
		}

		public String getClusterName() {
			return clusterName;
		}

		public void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}

		public int size() {
			int s = 0;
			try {
				s = Util.objectToByteBuffer(clusterName).length;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return s;
		}

		public void writeTo(DataOutput out) throws Exception {
			Util.writeString(clusterName, out);
			// out.write(Util.objectToByteBuffer(clusterName));
		}

		public void readFrom(DataInput in) throws Exception {
			clusterName = Util.readString(in);
			// clusterName = (String) Util.objectFromStream(in);
		}

		public String toString() {
			return "clusterName = " + clusterName;
		}
	}

}
