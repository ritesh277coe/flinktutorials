package com.ritesh;


import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PersonTest {
    @Test
    public void test1() throws IOException {
        Person p = Person.newBuilder()
                .setAge(10)
                .setEmail("a@b.com")
                .setHeight(6.1)
                .build();

        OutputStream object = new FileOutputStream("/Users/riteshsingh/riteshperson");
        p.writeDelimitedTo(object);

        p = Person.newBuilder()
                .setAge(11)
                .setEmail("a1@b.com")
                .setHeight(5.1)
                .build();
        p.writeDelimitedTo(object);

        object.close();


        FileInputStream inStream = new FileInputStream("/Users/riteshsingh/riteshperson");
        Person pFromFile = Person.parseDelimitedFrom(inStream);
        assertEquals(pFromFile.getAge(), 10);
        assertEquals(pFromFile.getEmail(), "a@b.com");
        assertEquals(pFromFile.getHeight(), 6.1);

        pFromFile = Person.parseDelimitedFrom(inStream);
        assertEquals(pFromFile.getAge(), 11);
        assertEquals(pFromFile.getEmail(), "a1@b.com");
        assertEquals(pFromFile.getHeight(), 5.1);

        inStream.close();

        System.out.println(p.toString());
    }
}
