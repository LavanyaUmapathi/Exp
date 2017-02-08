package com.rackspace.volga.etl.common.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

/**
 * User: alex.silva
 * Date: 4/14/15
 * Time: 7:56 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class JacksonConverterTest {

    String json = "{\"name\":{\"first\" : \"Joe\", \"last\" : \"Sixpack\" },\"gender\" : \"MALE\"," +
            "\"verified\" : false,\"userImage\" : \"Rm9vYmFyIQ==\"}";

    @Test(expected = RuntimeException.class)
    public void whenExceptionIsThrown() throws Exception {
        JacksonConverter<User> converter = new JacksonConverter<User>(User.class);
        ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
        Mockito.when(mapper.readValue(json, User.class)).thenThrow(IOException.class);
        ReflectionTestUtils.setField(converter, "objectMapper", mapper);
        converter.convertFrom(json, null);
    }

    @Test(expected = RuntimeException.class)
    public void whenExceptionIsThrown2() throws Exception {
        JacksonConverter<User> converter = new JacksonConverter<User>(User.class);
        ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
        Mockito.when(mapper.writeValueAsString(Mockito.any())).thenThrow(IOException.class);
        ReflectionTestUtils.setField(converter, "objectMapper", mapper);
        converter.convertTo(null);
    }

    @Test
    public void whenConvertingBackToJson() throws IOException {
        JacksonConverter<User> converter = new JacksonConverter<User>(User.class);
        User user = converter.convertFrom(json, new User());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tree1 = mapper.readTree(json);
        JsonNode tree2 = mapper.readTree(converter.convertTo(user));
        Assert.assertEquals(tree1, tree2);
    }

    @Test
    public void whenReadingJsonIntoExistingObject() {
        JacksonConverter<User> converter = new JacksonConverter<User>(User.class);
        User user = converter.convertFrom(json, new User());
        Assert.assertEquals(User.Gender.MALE, user.getGender());
        Assert.assertEquals("Joe", user.getName().getFirst());
        Assert.assertEquals("Sixpack", user.getName().getLast());
        Assert.assertEquals("Rm9vYmFyIQ==", new String(Base64.encodeBase64(user.getUserImage())));
        Assert.assertEquals(false, user.isVerified());
    }

    @Test
    public void whenReadingJsonIntoNullObject() {
        JacksonConverter<User> converter = new JacksonConverter<User>(User.class);
        User user = converter.convertFrom(json, null);
        Assert.assertEquals(User.Gender.MALE, user.getGender());
        Assert.assertEquals("Joe", user.getName().getFirst());
        Assert.assertEquals("Sixpack", user.getName().getLast());
        Assert.assertEquals("Rm9vYmFyIQ==", new String(Base64.encodeBase64(user.getUserImage())));
        Assert.assertEquals(false, user.isVerified());
    }

    private static class User {
        public enum Gender {MALE, FEMALE}

        ;

        public static class Name {
            private String _first, _last;

            public String getFirst() {
                return _first;
            }

            public String getLast() {
                return _last;
            }

            public void setFirst(String s) {
                _first = s;
            }

            public void setLast(String s) {
                _last = s;
            }
        }

        private Gender _gender;

        private Name _name;

        private boolean _isVerified;

        private byte[] _userImage;

        public Name getName() {
            return _name;
        }

        public boolean isVerified() {
            return _isVerified;
        }

        public Gender getGender() {
            return _gender;
        }

        public byte[] getUserImage() {
            return _userImage;
        }

        public void setName(Name n) {
            _name = n;
        }

        public void setVerified(boolean b) {
            _isVerified = b;
        }

        public void setGender(Gender g) {
            _gender = g;
        }

        public void setUserImage(byte[] b) {
            _userImage = b;
        }
    }

}