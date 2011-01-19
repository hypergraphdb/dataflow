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

import java.io.ByteArrayOutputStream;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.SerializerFactory;
import org.codehaus.jackson.map.SerializerProvider;
import org.hypergraphdb.peer.Structs;
import org.hypergraphdb.peer.Structs.StructsMapper;
import org.hypergraphdb.peer.serializer.JSONWriter;
import org.hypergraphdb.util.Pair;

public class JacksonSerializerFactory extends SerializerFactory
{
    SerializerFactory parent;
    Set<Class<?>> javaSerializedClasses = new HashSet<Class<?>>();
    
    public JacksonSerializerFactory(SerializerFactory parent)
    {
        this.parent = parent;
    }
    
    public Set<Class<?>> getJavaSerializedClasses()
    {
    	return this.javaSerializedClasses;
    }
    
    @Override
    public <T> JsonSerializer<T> createSerializer(Class<T> type,
                                                  SerializationConfig config)
    {
        if (this.javaSerializedClasses.contains(type))
            return new JavaPlatformSerializer<T>();
        Pair<StructsMapper, String> p = Structs.getMapper(type);
        if (p == null)
            return parent.createSerializer(type, config);
        else
            return new HGDBJsonSerializer<T>(p);
    }

    public static class HGDBJsonSerializer<T> extends JsonSerializer<T>
    {
        Pair<StructsMapper, String> p;
        
        HGDBJsonSerializer(Pair<StructsMapper, String> p) { this.p = p; }

        @Override
        public void serialize(Object value, JsonGenerator jgen,
                              SerializerProvider provider) throws IOException,
                                                                  JsonProcessingException
        {
            Object struct = p.getFirst().getStruct(value);
            jgen.writeString(new JSONWriter().write(struct));
        }
    }
    
    public static class JavaPlatformSerializer<T> extends JsonSerializer<T>
    {
        @Override
        public void serialize(Object value, JsonGenerator jgen,
                              SerializerProvider provider) throws IOException,
                                                                  JsonProcessingException
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOut = new ObjectOutputStream(out);
            objectOut.writeObject(value);        
            objectOut.flush();            
            jgen.writeBinary(out.toByteArray());
        }        
    }
}
