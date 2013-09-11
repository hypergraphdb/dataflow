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

import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Messages;
import org.hypergraphdb.peer.SubgraphManager;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.Activity;
import org.hypergraphdb.peer.workflow.WorkflowState;
import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Performative.*;

/**
 * <p>
 * This activity is initiated by a peer that has decided to join in the processing
 * of a particular data-flow network. The initiates a negotiation  
 * network. The initiating peer offers a target peer to join the network. If the
 * target already knows about this network, it just accepts the proposal and the
 * activity is completed. Otherwise it queries for the network information and
 * the activity completes once the network HGDB atom is transferred successfully.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class JoinNetworkActivity extends Activity
{
    public static final String TYPENAME = "disko-join-network";
    
    private HGHandle networkHandle;
    private HGPeerIdentity target;
    
    public JoinNetworkActivity(HyperGraphPeer thisPeer, HGHandle networkHandle, HGPeerIdentity target)
    {
        super(thisPeer);
        this.networkHandle = networkHandle;
        this.target = target;
    }

    public JoinNetworkActivity(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }
    
    @Override
    public void initiate()
    {
        Json msg = createMessage(Performative.Propose, this);
        msg.set(CONTENT, networkHandle);
        post(target, msg);
    }
    
    @Override
    public void handleMessage(Json message)
    {
        Json reply = null;
        Performative perf = Performative.toConstant(message.at("performative").asString());
        if (perf == Propose)
        {
            HGHandle h = Messages.fromJson(message.at(CONTENT));                
            if (getThisPeer().getGraph().get(h) != null)
            {
                reply = getReply(message, Performative.AcceptProposal);
                getState().assign(WorkflowState.Completed);
            }
            else 
                reply = getReply(message, Performative.QueryRef);                
        }
        else if (perf == AcceptProposal) // Our proposal was accepted, nothing else to do.
        {
            getState().assign(WorkflowState.Completed);
        }
        else if (perf == QueryRef)
        {
            reply = getReply(message, Performative.InformRef);
            reply.set(CONTENT, 
                      SubgraphManager.getTransferAtomRepresentation(getThisPeer().getGraph(), 
                                                                    networkHandle));
        }
        else if (perf == InformRef)
        {
            try
            {
                SubgraphManager.writeTransferedGraph(message.at(CONTENT), 
                                                    getThisPeer().getGraph());
                reply = getReply(message, Performative.AcceptProposal);
                getState().assign(WorkflowState.Completed);
            }
            catch (ClassNotFoundException ex)
            {
                throw new RuntimeException(ex);
            }
        }
        else if (perf == NotUnderstood)
        {
            getState().assign(WorkflowState.Failed);
        }
        else
        {
            reply = getReply(message, Performative.NotUnderstood);
            getState().assign(WorkflowState.Failed);
        }
        if (reply != null)
            post(getSender(message), reply);
    }
    
    @Override
    public String getType()
    {
        return TYPENAME;
    }
}