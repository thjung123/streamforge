package com.streamforge.core.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    static class Dummy {
        public String name;
        public int age;
    }

    @Test
    @DisplayName("toJson() should serialize object to JSON string")
    void testToJson() {
        Dummy d = new Dummy();
        d.name = "Alice";
        d.age = 30;

        String json = JsonUtils.toJson(d);
        assertTrue(json.contains("Alice"));
        assertTrue(json.contains("30"));
    }

    @Test
    @DisplayName("fromJson() should deserialize JSON string to object")
    void testFromJson() {
        String json = "{\"name\":\"Bob\",\"age\":25}";
        Dummy d = JsonUtils.fromJson(json, Dummy.class);

        assertEquals("Bob", d.name);
        assertEquals(25, d.age);
    }

    @Test
    @DisplayName("fromJson() should throw RuntimeException for invalid JSON")
    void testFromJsonInvalid() {
        assertThrows(RuntimeException.class, () -> {
            JsonUtils.fromJson("INVALID", Dummy.class);
        });
    }
}
