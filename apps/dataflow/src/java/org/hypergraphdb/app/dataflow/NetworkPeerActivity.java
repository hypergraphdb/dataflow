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

import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import java.util.UUID;

import static org.hypergraphdb.peer.Messages.*;
import static org.hypergraphdb.peer.Structs.*;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.BeanDeserializerFactory;
import org.codehaus.jackson.map.deser.StdDeserializerProvider;
import org.codehaus.jackson.map.ser.BeanSerializerFactory;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.workflow.FSMActivity;
import org.hypergraphdb.peer.workflow.FromState;
import org.hypergraphdb.peer.workflow.OnMessage;
import org.hypergraphdb.peer.workflow.WorkflowState;
import org.hypergraphdb.peer.workflow.WorkflowStateConstant;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * Handles all peer communication pertaining to a given DFN that might
 * currently be running or not. The UUID of a
 * <code>NetworkPeerActivity</code> is the same as the persistent
 * handle of the <code>DataFlowNetwork</code> atom.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class NetworkPeerActivity extends FSMActivity
{
    public static final String TYPENAME = "dataflow-activity";
    
    public WorkflowStateConstant NetworkRunning = WorkflowState.makeStateConstant("NetworkRunning");
    
    private HGHandle networkHandle;
    private DataFlowNetwork<?> network;

    private ObjectMapper jsonObjectMapper = null;
    
    private ObjectMapper getObjectMapper()
    {
        if (jsonObjectMapper == null)
        {            
            jsonObjectMapper = new ObjectMapper();        
            jsonObjectMapper.setSerializerFactory(
                new JacksonSerializerFactory(BeanSerializerFactory.instance));
            jsonObjectMapper.setDeserializerProvider(new StdDeserializerProvider(
                new JacksonDeserializerFactory(BeanDeserializerFactory.instance)));
        }
        return jsonObjectMapper;
    }
    
    @SuppressWarnings("unchecked")
    Map<String, Object> toJson(Object x)
    {
        Mapping<Object, String> ser = 
            (Mapping<Object, String>)this.getThisPeer().getObjectContext().get("dataflow-json-serializer");
        String serialized = null;
        if (ser != null)
            serialized = ser.eval(x);
        else
        {
            StringWriter jsonWriter = new StringWriter();
            try
            {
                getObjectMapper().writeValue(jsonWriter, x);
            }
            catch (Throwable e)
            {
                throw new RuntimeException(e);
            }
            serialized = jsonWriter.toString();
        }
        return struct("classname", x.getClass().getName(),
               "json", serialized);
    }
    
    @SuppressWarnings("unchecked")
    <T> T fromJson(Map<String, Object> structure)
    {
        Mapping<Map<String, Object>, T> ser = 
            (Mapping<Map<String, Object>, T>)this.getThisPeer().getObjectContext().get("dataflow-json-deserializer");
        if (ser != null)
            return ser.eval(structure);
        else
        {
            Class<?> clazz;
            try
            {
                clazz = HGUtils.loadClass(getThisPeer().getGraph(),
                                                   (String)getPart(structure, "classname"));
                return (T)getObjectMapper().readValue((String)getPart(structure, "classname"), clazz);                
            }
            catch (Exception e)
            {
                throw new HGException(e);
            }               
        }
    }
    
    private Channel<?> getChannel(Object channelDesc)
    {
        if (channelDesc instanceof List)
        {
            HGHandle jobId = getPart(channelDesc, 0);            
            String channelId = getPart(channelDesc, 1);
            Channel<?> logicalChannel = network.getChannel(channelId);
            JobDataFlow<?> jobNetwork = (JobDataFlow<?>)network;
            return jobNetwork.getChannelManager().getJobChannel(jobNetwork, 
                                                                logicalChannel, 
                                                                (Job)getThisPeer().getGraph().get(jobId));
        }
        else
            return network.getChannel(channelDesc.toString());
    }
    
    public NetworkPeerActivity(HyperGraphPeer thisPeer, HGHandle network)
    {
        super(thisPeer,
              UUID.fromString(thisPeer.getGraph().getPersistentHandle(network).toString()));
        this.networkHandle = network;
    }
    
    public NetworkPeerActivity(HyperGraphPeer thisPeer, UUID activityId)
    {
        super(thisPeer, activityId);
        if (activityId != null)
            networkHandle = thisPeer.getGraph().getHandleFactory().makeHandle(activityId.toString());
    }    
    
    public void initiate()
    {
        network = getThisPeer().getGraph().get(networkHandle);
        
        //
        // Broadcast to all peers that we are starting up this network at this location. 
        //
        getPeerInterface().broadcast(createMessage(Performative.CallForProposal, this));
    }
    
    @FromState("Started")
    @OnMessage(performative="CallForProposal")
    public WorkflowState onCallForProposal(Message msg)
    {
        // If we have a copy of the network locally, send a proposal
        // to participate, otherwise reject it. Eventually, the proposal
        // might include relevant information about this peer such
        // computing resources available etc.
        network = getThisPeer().getGraph().get(networkHandle);
        if (network != null)
        {
            reply(msg, Performative.Propose, null);
            return null;
        }
        else
            return WorkflowState.Canceled;
    }

    @FromState("Started")
    @OnMessage(performative="Propose")
    public WorkflowState onPropose(Message msg)
    {
        reply(msg, Performative.AcceptProposal, null);
        return NetworkRunning;
    }
    
    @FromState("Started")
    @OnMessage(performative="AcceptProposal")
    public WorkflowState onAcceptPropose(Message msg)
    {
        return NetworkRunning;
    }
    
    /**
     * Handles new data arriving at some channel. 
     */
    @SuppressWarnings("unchecked")
    @FromState("NetworkRunning")
    @OnMessage(performative="InformRef")
    public WorkflowState onNewData(Message msg) throws Exception
    {
        Object channelId = getPart(msg, CONTENT, "channel");
        if (channelId != null)
        {
            DistributedChannel<Object> ch = (DistributedChannel<Object>)getChannel(channelId);
            Map<String, Object> rep = getPart(msg, CONTENT, "datum"); 
            ch.putLocal(fromJson(rep));
            reply(msg, Performative.Confirm, null);
        }
        return null;
    }

    @FromState("NetworkRunning")
    @OnMessage(performative="Confirm")
    public WorkflowState onDataConfirm(Message msg) throws Exception
    {
        return null;
    }
    
    @Override
    public String getType()
    {
        return TYPENAME;
    }
}
