package org.ritesh;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import com.google.protobuf.Message;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Parser;
import java.util.List;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.Exception;
import java.lang.reflect.Method;
import java.lang.annotation.Target;

import com.ritesh.Person;
import com.ritesh.PersonProto;

public class GenericBinaryProtoDeserializer<T extends GeneratedMessage> implements DeserializationSchema<T> {

    private T defaultInstance;
    private final TypeInformation<T> typeInfo;

    // Transient types we only use for caching
    private transient Parser<T> parser;

    public GenericBinaryProtoDeserializer(Class<T> targetType) {
        this.typeInfo = TypeInformation.of(targetType);
        try {
            this.defaultInstance = (T) targetType.getMethod("getDefaultInstance").invoke(null);
        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("Failed to get parser");
        }
    }

    private Parser<T> getParser() {
        if (this.parser == null) {
            this.parser = (Parser<T>) this.defaultInstance.getParserForType();
        }
        return this.parser;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        try {
            Parser<T> parser = this.getParser();
            if (parser == null) {
                throw new IOException("No parser for given message");
            }

            //return parser.parseFrom(message);
            InputStream inputStream = new ByteArrayInputStream(message);
            return parser.parseDelimitedFrom(inputStream);
        } catch (Exception e) {
            System.out.println(e.toString());
            throw new IOException("Unable to deserialize bytes");
        }
    }


    @Override
    public boolean isEndOfStream(GeneratedMessage nextElement) {
        // This can be overwritten when testing to end the stream when a last element is received
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.typeInfo;
    }
}
