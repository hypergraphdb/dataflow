/*******************************************************************************
 * Copyright (c) 2011 Kobrix Software, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser Public License v2.1
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *    Borislav Iordanov, Murilo Saraiva de Queiroz - initial API and implementation
 ******************************************************************************/
package org.hypergraphdb.app.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.PossibleOutcome;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * Transfer a data-flow processor from one network peer to another. Such
 * transfers occurs either while the network is idle or it is paused
 * in between jobs. The transfer is a relatively complex process and
 * it involves peers that hold data-flow processors in the neighborhood 
 * of the processor being transfered - each peer running  a processor
 * with channels connected to the processor being transfered must be
 * informed of the transfer in order to appropriately redirect the flow
 * of data through those channels. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class TransferProcessorActivity extends FSMActivity
{
    public static final String TYPENAME = "disko-transfer-processor";
    
    // We use a couple of additional states to manage interaction.
    
    /**
     * State indicating that we are in the process of transferring the channels used
     * by the processor.
     */
    public static final WorkflowStateConstant ChannelTransfer = 
        WorkflowState.makeStateConstant("ChannelTransfer");
    /**
     * State indicating that we are in the process of transferring processors 
     * connected to this processor as consumers of data produced by it (i.e.
     * connected to the output channels of the main processor being transferred.
     */
    public static final WorkflowStateConstant SiblingTransfer = 
        WorkflowState.makeStateConstant("SiblingTransfer");
    
    private HGHandle processor;
    private HGHandle network;
    private HGPeerIdentity target;
    
    // Sibling processors are transferred one by one and the following field
    // keeps the count making sure that all were transferred. No need to
    // synchronize because a single activity never runs concurrently with
    // itself.
    private int remainingSiblings = 0;

    public TransferProcessorActivity(HyperGraphPeer thisPeer, 
                                     HGHandle processor, 
                                     HGHandle network,
                                     HGPeerIdentity target)
    {
        super(thisPeer);
        this.processor = processor;
        this.network = network;
        this.target = target;
    }
    
    public TransferProcessorActivity(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    /**
     * Initiated by the peer currently holding the processor - that peer offers
     * to transfer the processor to the destination.
     */
    @Override
    public void initiate()
    {
        Message msg = createMessage(Performative.Propose, this);
        combine(msg, struct(CONTENT, struct("processor", processor, "network", network)));
        post(target, msg);
    }
    
    /**
     * The destination peer examines the proposal and accepts iff it knows about the
     * network and the processor is not already running locally.
     */
    @FromState("Started")
    @OnMessage(performative="Propose")
    public WorkflowState onPropose(Message msg)
    {
        processor = getPart(msg, CONTENT, "processor");
        network = getPart(msg, CONTENT, "network");
        if (processor == null || network == null)
        {
            post(getSender(msg), getReply(msg, Performative.NotUnderstood, "processor or network null"));
            return WorkflowState.Failed;
        }
        else
        {
            // Some validation...
            String error = null;
            // Check that we know about the network.
            if (getThisPeer().getGraph().get(network) == null)
                error = "unknown network";
            // Check that the processor is not already running locally
            if (!hg.findAll(getThisPeer().getGraph(), 
                            hg.and(hg.type(HGPlainLink.class),
                                   hg.orderedLink(network, 
                                                  processor, 
                                                  getThisPeer().getIdentity().getId()))).isEmpty())
                error = "processor already running locally";
            if (error != null)
            {
                post(getSender(msg), getReply(msg, Performative.NotUnderstood, error));
                return WorkflowState.Canceled;
            }
            else
            {
                post(getSender(msg), getReply(msg, Performative.AcceptProposal));
                return null;
            }
        }
    }

    /**
     * The source receives a proposal acceptance and simply send the processor
     * as a HGDB atom with an InformRref. 
     */
    @FromState("Started")
    @OnMessage(performative="AcceptProposal")
    public WorkflowState onAcceptProposal(Message msg)
    {
        Message reply = getReply(msg, Performative.InformRef);
        combine(reply, struct(CONTENT, 
                              SubgraphManager.getTransferAtomRepresentation(getThisPeer().getGraph(), 
                                                                            processor)));
        post(getSender(msg), reply);
        return null;
    }
    
    @FromState("Started")
    @OnMessage(performative="InformRef")
    public WorkflowState onProcessorInformRef(final Message msg) throws Exception
    {
        final Object atom = getPart(msg, CONTENT);
        if (atom == null)
        {
            post(getSender(msg), getReply(msg, Performative.Failure, "missing processor atom"));
            return WorkflowState.Failed;
        }        
        return getThisPeer().getGraph().getTransactionManager().transact(new Callable<WorkflowState>() {
            public WorkflowState call() throws Exception
            {
                // Store the processor atom itself locally, overwriting any previous version
                HGHandle cHandle = SubgraphManager.writeTransferedGraph(
                                        atom, 
                                        getThisPeer().getGraph()).iterator().next();
                
                if (!cHandle.equals(processor))
                    throw new Exception("Wrong processor received '" + cHandle + "', expected " + processor);
                DistUtils.setOwningPeer(getThisPeer().getGraph(), 
                                        network, 
                                        processor, 
                                        getThisPeer().getIdentity().getId());
                //
                // Ask for all channels connected to this processor.
                //
                post(getSender(msg), 
                     getReply(msg, 
                              Performative.QueryRef, 
                              list("channels-of", processor)));                
                return ChannelTransfer;
            }
        });
    }
    
    @FromState("Started")
    @OnMessage(performative="QueryRef")
    @PossibleOutcome("ChannelTransfer")
    public WorkflowState onQueryRef(Message msg) throws Exception
    {
        String kind = getPart(msg, CONTENT, 0);
        assert kind != null : new NullPointerException("missing query-ref kind");
        HGHandle proc = getPart(msg, CONTENT, 1);
        assert proc !=  null : new NullPointerException("missing processor handle");
        if ("channels-of".equals(kind))
        {
            List<HGHandle> channelLinks = hg.findAll(getThisPeer().getGraph(), 
                                                     hg.and(hg.type(ChannelLink.class), 
                                                            hg.incident(proc)));
            List<Object> channels = new ArrayList<Object>();
            for (HGHandle ch : channelLinks)
            {
                ChannelLink<?> theLink = getThisPeer().getGraph().get(ch);
                System.out.println("Transfer link " + theLink.getChannel());
                channels.add(SubgraphManager.getTransferAtomRepresentation(
                                     getThisPeer().getGraph(), ch));
            }
            reply(msg, Performative.InformRef, channels);
            return ChannelTransfer;
        }
        else
        {
            reply(msg, Performative.NotUnderstood, "unrecognized query-ref kind");
            return WorkflowState.Failed;
        }
    }
    
    @FromState("ChannelTransfer")
    @OnMessage(performative="InformRef")
    @PossibleOutcome("SiblingTransfer")
    public WorkflowState onChannelsInformRef(Message msg) throws Exception
    {
        List<Object> channels = getPart(msg, CONTENT);
        for (Object atom : channels)
        {
            // Do we really want to overwrite possibly already stored channels here?
            HGHandle chHandle = SubgraphManager.writeTransferedGraph(
                                    atom, 
                                    getThisPeer().getGraph()).iterator().next();
            ChannelLink<?> chLink = getThisPeer().getGraph().get(chHandle);
            // Get all sibling processors            
            for (HGHandle h : chLink)
                if (getThisPeer().getGraph().get(h) == null)
                {
                    remainingSiblings++;
                    reply(msg, Performative.QueryRef, h);
                }
        }
        return SiblingTransfer;
    }

    @FromState({"ChannelTransfer", "SiblingTransfer"})
    @OnMessage(performative="QueryRef")
    @PossibleOutcome("SiblingTransfer")
    public WorkflowState onSiblingQueryRef(Message msg) throws Exception
    {
        HGHandle sibling = getPart(msg, CONTENT);
        // Where is this sibling running?
        HGHandle owningPeer = hg.findOne(getThisPeer().getGraph(), 
                                         hg.apply(hg.targetAt(getThisPeer().getGraph(), 2),
                                                  hg.orderedLink(network, sibling, hg.anyHandle()))); 
        reply(msg, 
              Performative.InformRef, 
              list(SubgraphManager.getTransferAtomRepresentation(getThisPeer().getGraph(), 
                                                                 sibling),
                   owningPeer));
        return SiblingTransfer;
    }

    @FromState("SiblingTransfer")
    @OnMessage(performative="InformRef")
    @PossibleOutcome({"SiblingTransfer", "Completed"})
    public WorkflowState onSiblingReceive(final Message msg) throws Exception
    {
        if (remainingSiblings <= 0)
        {
            reply(msg, Performative.Failure, "wrong sibling count");
            return WorkflowState.Failed;
        }
        final Object atom = getPart(msg, CONTENT, 0);
        final HGHandle owningPeer = getPart(msg, CONTENT, 1);
        return getThisPeer().getGraph().getTransactionManager().transact(new Callable<WorkflowState>() {
            public WorkflowState call() throws Exception
            {
                HGHandle h = SubgraphManager.writeTransferedGraph(
                                 atom, 
                                 getThisPeer().getGraph()).iterator().next();
                
                DistUtils.setOwningPeer(getThisPeer().getGraph(), network, h, owningPeer);
                //
                // Once we finish transfer of all sibling processors, we are almost done. All that remains
                // is to inform all sibling peers that the processor has moved and then signal to the
                // originating peer (the one that send us the processor) that the task is complete as
                // far as we are concerned.
                //
                if (--remainingSiblings == 0) 
                {
                    HGBreadthFirstTraversal traversal = new HGBreadthFirstTraversal(processor, 
                            new DefaultALGenerator(getThisPeer().getGraph(), hg.type(ChannelLink.class), null), 1);
                    Message informLocation = createMessage(Performative.Inform, TransferProcessorActivity.this);
                    combine(informLocation, struct("token", "processor-location", 
                                                   "network", network,
                                                   "processor", processor,
                                                   "location", getThisPeer().getIdentity().getId()));
                    while (traversal.hasNext())
                    {
                        Pair<HGHandle, HGHandle> current = traversal.next();
                        System.out.println("Looking for peer location for " + current.getSecond());
                        HGPeerIdentity peerLocation = DistUtils.getOwningPeer(getThisPeer().getGraph(),
                                                                              network, 
                                                                              current.getSecond());
                        System.out.println("Peer location " + peerLocation);
                        if (peerLocation == null)
                            throw new NullPointerException("Unknown peer location for processor: " + current.getSecond());
                        else
                            send(peerLocation, informLocation); // blocking send - will fail the whole activity if unsuccessful
                    }
                    reply(msg, Performative.Inform, struct("token", "done"));
                    return WorkflowState.Completed;
                }
                else
                    return SiblingTransfer;                
            }
        });
    }
    
    @FromState({"SiblingTransfer", "ChannelTransfer", "Started"})
    @OnMessage(performative="Inform")
    public WorkflowState onInformGeneric(Message msg)
    {
        String token = getPart(msg, CONTENT, "token");
        if ("done".equals(token))
            return WorkflowState.Completed;
        else if ("processor-location".equals(token))
        {
            HGHandle net = getPart(msg, CONTENT, "network");
            HGHandle proc = getPart(msg, CONTENT, "processor");
            HGHandle peer = getPart(msg, CONTENT, "location");
            DistUtils.setOwningPeer(getThisPeer().getGraph(), net, proc, peer);
            
            // If this is just an informational blurb...
            // Perhaps such blurbs should be made into a separate activity rather
            // than being embedded as side-effect to TransferProcessor which is a rather
            // complex conversation mostly b/w only two peers (except for this 
            // processor-location blurb
            if (getState().equals(WorkflowState.Started))
                return WorkflowState.Completed;
        }
        return null;
    }
    
    @Override
    public String getType()
    {
        return TYPENAME;
    }
}
