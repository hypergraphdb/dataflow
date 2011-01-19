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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.DeserializerFactory;
import org.codehaus.jackson.map.DeserializerProvider;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.type.ArrayType;
import org.codehaus.jackson.map.type.CollectionType;
import org.codehaus.jackson.map.type.MapType;
import org.codehaus.jackson.type.JavaType;
import org.hypergraphdb.peer.Structs;
import org.hypergraphdb.peer.Structs.StructsMapper;
import org.hypergraphdb.peer.serializer.JSONReader;
import org.hypergraphdb.type.SerializableType.SerInputStream;
import org.hypergraphdb.util.Pair;

public class JacksonDeserializerFactory extends DeserializerFactory
{
    DeserializerFactory parent;    
    Set<Class<?>> javaSerializedClasses = new HashSet<Class<?>>();
    
    public Set<Class<?>> getJavaSerializedClasses()
    {
    	return this.javaSerializedClasses;
    }
    
    public static class HGDBJsonDeserializer<T> extends JsonDeserializer<T>
    {
        Pair<StructsMapper, String> p;
        
        HGDBJsonDeserializer(Pair<StructsMapper, String> p) { this.p = p; }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException
        {
            String text =  jp.readValueAs(String.class);
            Object x = new JSONReader().read(text);
            return (T) p.getFirst().getObject(x);
        }
    }

    public static class JavaPlatformDeserializer<T> extends JsonDeserializer<T>
    {
        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException
        {
            byte [] A = jp.readValueAs(byte[].class);
            ByteArrayInputStream in = new ByteArrayInputStream(A);
            ObjectInputStream objectIn = new SerInputStream(in, null);        
            try
            {
                return (T) objectIn.readObject();
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }            
        }
    }

   
    public JacksonDeserializerFactory(DeserializerFactory parent)
    {
        this.parent = parent;
    }
    
    @Override
    public JsonDeserializer<?> createArrayDeserializer(DeserializationConfig config,
                                                       ArrayType type,
                                                       DeserializerProvider p)
            throws JsonMappingException
    {
        return parent.createArrayDeserializer(config, type, p);
    }

    @Override
    public JsonDeserializer<Object> createBeanDeserializer(DeserializationConfig config,
                                                           JavaType type,
                                                           DeserializerProvider p)
            throws JsonMappingException
    {
        if (this.javaSerializedClasses.contains(type.getRawClass()))
            return new JavaPlatformDeserializer<Object>();
        
        Pair<StructsMapper, String> mapper = Structs.getMapper(type.getRawClass());
        if (mapper == null)
            return parent.createBeanDeserializer(config, type, p);
        else
            return new HGDBJsonDeserializer<Object>(mapper);
    }

    @Override
    public JsonDeserializer<?> createCollectionDeserializer(DeserializationConfig config,
                                                            CollectionType type,
                                                            DeserializerProvider p)
            throws JsonMappingException
    {
        return parent.createCollectionDeserializer(config, type, p);
    }

    @Override
    public JsonDeserializer<?> createEnumDeserializer(DeserializationConfig config,
                                                      Class<?> enumClass,
                                                      DeserializerProvider p)
            throws JsonMappingException
    {
        return parent.createEnumDeserializer(config, enumClass, p);
    }

    @Override
    public JsonDeserializer<?> createMapDeserializer(DeserializationConfig config,
                                                     MapType type,
                                                     DeserializerProvider p)
            throws JsonMappingException
    {
        return parent.createMapDeserializer(config, type, p);
    }

    @Override
    public JsonDeserializer<?> createTreeDeserializer(DeserializationConfig config,
                                                      Class<? extends JsonNode> nodeClass,
                                                      DeserializerProvider p)
            throws JsonMappingException
    {
        return parent.createTreeDeserializer(config, nodeClass, p);
    }
}
