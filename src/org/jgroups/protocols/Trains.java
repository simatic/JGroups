package org.jgroups.protocols;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
		 * Test inputFile parameter and read inputFile
		 */
		System.out.println("inputFile = " + inputFile);

		// // Z means:
		// "The end of the input but for the final terminator, if any"
		// @SuppressWarnings("resource")
		// String output = new Scanner(new File("conf/" + inputFile))
		// .useDelimiter("\\Z").next();
		// System.out.println("Configuratinon: " + output);

		/*
		 * Load JNI lib //
		 */
		// System.setProperty(
		// "java.library.path",
		// "/home/wang/workspace/PFE/TrainsProtocolJava/TrainsProtocol/lib:/home/wang/workspace/PFE/TrainsProtocolJava/TrainsProtocol/lib::/home/wang/workspace/PFE/TrainsProtocolJava/in:/home/wang/workspace/PFE/TrainsProtocolJava/bin:/usr/java/packages/lib/amd64:/usr/lib/x86_64-linux-gnu/jni:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu:/usr/lib/jni:/lib:/usr/lib");
		System.out.println("lib path = "
				+ System.getProperty("java.library.path"));

		// System.load("/mci/ei0912/wang_tie/workspace/JGroups/lib/TrainsJniProxy.so");
		// System.loadLibrary("trains");

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
		 * Test JNI proxy functionality
		 */
		// System.out.println(trains.getMessageFrom("sender"));

		/*
		 * thread pool (why?)
		 */
		// default_pool = getTransport().getDefaultThreadPool();
		// oob_pool = getTransport().getOOBThreadPool();

		/*
		 * trInit
		 */
		// trains.trInit(trainsNumber, wagonLength, waitNb, waitTime);
	}

	public void start() throws Exception {
		System.out.println("Trains start");

		super.start();
		running = true;

		/*
		 * test message
		 */
		// Message msg = new Message(null, local_addr, "message-1");
		// Event evt = new Event(Event.MSG, msg);
		// this.up(evt);
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
		System.out.println("Trains down");
		int exitcode = 0;
		
		switch (evt.getType()) {
		case Event.CONNECT:
		case Event.CONNECT_USE_FLUSH:
		case Event.CONNECT_WITH_STATE_TRANSFER:
		case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
			System.out.println("Trains down connect type = " + evt.getType());
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
			
			System.out.println("Trains down message");

			Message msg = (Message) evt.getArg();
			msg.setSrc(local_addr);
			System.out.println(msg.printHeaders());

			System.out.println("msg = " + msg);
			System.out.println("msg sent from " + msg.getSrc());

			trains.Message msgTrains;
			try {
				// String str = msg.getBuffer();
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
			System.out.println("Trains down unknown type = " + evt.getType());
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
		System.out.println("Trains up");

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
			System.out.println("Trains up unknown type = " + evt.getType());

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

			// Printing the new/departed participant
			if (cv.getJoined() != 0) {
				System.out.println(Integer.toString(cv.getJoined())
						+ " has arrived.");
			} else {
				System.out.println(Integer.toString(cv.getDeparted())
						+ " is gone.");
			}
			// Need cv -> members as an array
			// View view = new View(creator, id, members);
			// Event evt = new Event(Event.VIEW_CHANGE, view);
			// prot.up(evt);

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

			System.out
					.println(sender + "sent content" + msgTrains.getPayload());
			System.out.println("The content size is "
					+ msgTrains.getMessageHeader().getLen());
			Message msg = null;
			try {
				msg = (Message) Util.objectFromByteBuffer(msgTrains
						.getPayload());
				System.out.println("received from = " + msg.getSrc());
				System.out.println("received msg = " + msg);
				System.out
						.println("The content is " + (String) msg.getObject());
				// msg = new Message(null, null, str);
				// System.out.println("type is " + msgTrains.getPayload()[0]);
				System.out.println("prot = " + prot);
				Event evt = new Event(Event.MSG, msg);
				prot.up(evt);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	private class AddressTrains implements Address {

		@Override
		public void writeTo(DataOutput out) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public int compareTo(Address o) {
			// TODO Auto-generated method stub
			return 0;
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
			return Global.INT_SIZE + this.clusterName.length();
		}

		public void writeTo(DataOutput out) throws Exception {
			out.write(Util.objectToByteBuffer(clusterName));
		}

		public void readFrom(DataInput in) throws Exception {
			clusterName = (String) Util.objectFromStream(in);
		}

		public String toString() {
			return "clusterName=" + clusterName;
		}
	}

}
