package org.jgroups.demos;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/** Demos RELAY. Create 2 *separate* clusters with RELAY as top protocol. Each RELAY has bridge_props="tcp.xml" (tcp.xml
 * needs to be present). Then start 2 instances in the first cluster and 2 instances in the second cluster. They should
 * find each other, and typing in a window should send the text to everyone, plus we should get 4 responses.
 * @author Bela Ban
 */
public class RelayDemoRpc extends ReceiverAdapter {
    protected JChannel      ch;
    protected RpcDispatcher disp;
    protected String        local_addr;
    protected View          view;



    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String name=null;


        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            System.out.println("RelayDemo [-props props] [-name name]");
            return;
        }
        RelayDemoRpc demo=new RelayDemoRpc();
        demo.start(props, name);
    }

    public void start(String props, String name) throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        disp=new RpcDispatcher(ch, null, this, this);
        ch.connect("RelayDemo");
        local_addr=ch.getAddress().toString();

        MethodCall call=new MethodCall(getClass().getMethod("handleMessage", String.class, String.class));
        for(;;) {
            String line=Util.readStringFromStdin(": ");
            call.setArgs(line, local_addr);

            // unicast to every member of the local cluster
            if(line.equalsIgnoreCase("unicast")) {
                for(Address dest: view.getMembers()) {
                    System.out.println("invoking method in " + dest + ": ");
                    try {
                        Object rsp=disp.callRemoteMethod(dest, call, new RequestOptions(ResponseMode.GET_ALL, 5000));
                        System.out.println("rsp from " + dest + ": " + rsp);
                    }
                    catch(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
            }

            // unicast to 1 SiteMaster
            else if(line.startsWith("site")) {
                Collection<String> site_masters=parseSiteMasters(line.substring("site".length()));
                for(String site_master: site_masters) {
                    SiteMaster dest=new SiteMaster(site_master);
                    System.out.println("invoking method in " + dest + ": ");
                    try {
                        Object rsp=disp.callRemoteMethod(dest, call, new RequestOptions(ResponseMode.GET_ALL, 50000));
                        System.out.println("rsp from " + dest + ": " + rsp);
                    }
                    catch(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
            }

            // mcast to all local members and N SiteMasters
            else if(line.startsWith("mcast")) {
                Collection<String> site_masters=parseSiteMasters(line.substring("mcast".length()));
                Collection<Address> dests=new ArrayList<Address>(site_masters.size());
                for(String site_master: site_masters)
                    dests.add(new SiteMaster(site_master));
                dests.addAll(view.getMembers());
                System.out.println("invoking method in " + dests + ": ");
                RspList<Object> rsps=disp.callRemoteMethods(dests, call,
                                                            new RequestOptions(ResponseMode.GET_ALL, 5000).setAnycasting(true));
                for(Rsp rsp: rsps.values()) {
                    if(rsp.wasUnreachable())
                        System.out.println("<< unreachable: " + rsp.getSender());
                    else
                        System.out.println("<< " + rsp.getValue() + " from " + rsp.getSender());
                }
            }
            else {
                // mcasting the call to all local cluster members
                RspList<Object> rsps=disp.callRemoteMethods(null, call, new RequestOptions(ResponseMode.GET_ALL, 5000).setAnycasting(false));
                for(Rsp rsp: rsps.values())
                    System.out.println("<< " + rsp.getValue() + " from " + rsp.getSender());
            }
        }
    }

    protected static Collection<String> parseSiteMasters(String line) {
        Set<String> retval=new HashSet<String>();
        String[] tmp=line.split("\\s");
        for(String s: tmp) {
            String result=s.trim();
            if(result.length() > 0)
                retval.add(result);
        }
        return retval;
    }

    public static String handleMessage(String msg, String sender) {
        System.out.println("<< " + msg + " from " + sender);
        return "this is a response";
    }


    public void viewAccepted(View new_view) {
        System.out.println(print(new_view));
        view=new_view;
    }


    static String print(View view) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        sb.append(view.getClass().getSimpleName() + ": ").append(view.getViewId()).append(": ");
        for(Address mbr: view.getMembers()) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(mbr);
        }
        return sb.toString();
    }
}