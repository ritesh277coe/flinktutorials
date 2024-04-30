package org.ritesh;

import com.google.protobuf.GeneratedMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.google.protobuf.Message;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Parser;

import java.io.*;
import java.util.List;
import java.util.LinkedList;
import java.lang.Exception;
import java.lang.reflect.Method;
import java.lang.annotation.Target;

import com.ritesh.Person;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class GenericBinaryProtoSerializer<T extends GeneratedMessage> implements KafkaRecordSerializationSchema<T> {

    private static final long serialVersionUID = 1L;
    private String topic;

    GenericBinaryProtoSerializer() {}
    GenericBinaryProtoSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(T message, KafkaSinkContext kafkaSinkContext, Long aLong) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            message.writeDelimitedTo(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] binary = stream.toByteArray();


        return new ProducerRecord<>(
                topic,
                null,
                System.currentTimeMillis() % 1000,
                null,
                binary);
    }

}
