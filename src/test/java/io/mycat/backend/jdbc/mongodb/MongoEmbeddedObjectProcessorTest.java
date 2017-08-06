package io.mycat.backend.jdbc.mongodb;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @author liuxinsi
 * @mail akalxs@gmail.com
 */
public class MongoEmbeddedObjectProcessorTest {
    @Test
    public void testValueMapperWithObjectId() {
        String id = "5978776b8d69f75e091067ed";

        Object obj = MongoEmbeddedObjectProcessor.valueMapper("_id", id, ObjectId.class);
        if (!(obj instanceof ObjectId)) {
            Assert.fail("not objectId");
        }
    }

    @Test
    public void testValueMapperWithEmbeddedObject() {
        BasicDBObject dbObj = new BasicDBObject();
        dbObj.put("str", "t1");
        dbObj.put("inte", 1);
        dbObj.put("date", new Date());
        dbObj.put("lon", 100L);
        dbObj.put("bool", true);
        dbObj.put("strs", new String[]{"a", "b", "c"});
        dbObj.put("intes", new Integer[]{1, 2, 3});
        dbObj.put("bytes", "ttt".getBytes());
        dbObj.put("b", "a".getBytes()[0]);

        Object o = MongoEmbeddedObjectProcessor.valueMapper("embObj", dbObj, TestObject.class);
        if (!(o instanceof TestObject)) {
            Assert.fail("not emb obj");
        }
    }

    @Test
    public void testValueMapperWithDeepEmbeddedObject() {
        BasicDBObject dbObj = new BasicDBObject();
        dbObj.put("str", "t1");
        dbObj.put("inte", 1);
        dbObj.put("date", new Date());
        dbObj.put("lon", 100L);
        dbObj.put("bool", true);
        dbObj.put("strs", new String[]{"a", "b", "c"});
        dbObj.put("intes", new Integer[]{1, 2, 3});
        dbObj.put("bytes", "ttt".getBytes());
        dbObj.put("b", "a".getBytes()[0]);

        BasicDBObject embedObj = new BasicDBObject();
        embedObj.put("embeddedStr", "e1");

        BasicDBObject deepEmbedObj1 = new BasicDBObject();
        deepEmbedObj1.put("str", "aaa");

        BasicDBObject deepEmbedObj2 = new BasicDBObject();
        deepEmbedObj2.put("str", "bbb");


        embedObj.put("testObjectList", Lists.newArrayList(deepEmbedObj1, deepEmbedObj2));

        dbObj.put("embeddedObject", embedObj);

        Object o = MongoEmbeddedObjectProcessor.valueMapper("embObj", dbObj, TestObject.class);
        if (!(o instanceof TestObject)) {
            Assert.fail("not emb obj");
        }
        System.out.println(o);
    }
}

class TestObject {
    private ObjectId _id;
    private String str;
    private Integer inte;
    private Date date;
    private Long lon;
    private Boolean bool;
    private String[] strs;
    private Integer[] intes;
    private byte[] bytes;
    private Byte b;
    private EmbeddedObject embeddedObject;

    public ObjectId get_id() {
        return _id;
    }

    public void set_id(ObjectId _id) {
        this._id = _id;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public Integer getInte() {
        return inte;
    }

    public void setInte(Integer inte) {
        this.inte = inte;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Long getLon() {
        return lon;
    }

    public void setLon(Long lon) {
        this.lon = lon;
    }

    public Boolean getBool() {
        return bool;
    }

    public void setBool(Boolean bool) {
        this.bool = bool;
    }

    public String[] getStrs() {
        return strs;
    }

    public void setStrs(String[] strs) {
        this.strs = strs;
    }

    public Integer[] getIntes() {
        return intes;
    }

    public void setIntes(Integer[] intes) {
        this.intes = intes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public Byte getB() {
        return b;
    }

    public void setB(Byte b) {
        this.b = b;
    }

    public EmbeddedObject getEmbeddedObject() {
        return embeddedObject;
    }

    public void setEmbeddedObject(EmbeddedObject embeddedObject) {
        this.embeddedObject = embeddedObject;
    }
}

class EmbeddedObject {
    private String embeddedStr;
    private List<TestObject> testObjectList;
    private Set<String> someCodeSet;

    public String getEmbeddedStr() {
        return embeddedStr;
    }

    public void setEmbeddedStr(String embeddedStr) {
        this.embeddedStr = embeddedStr;
    }

    public List<TestObject> getTestObjectList() {
        return testObjectList;
    }

    public void setTestObjectList(List<TestObject> testObjectList) {
        this.testObjectList = testObjectList;
    }

    public Set<String> getSomeCodeSet() {
        return someCodeSet;
    }

    public void setSomeCodeSet(Set<String> someCodeSet) {
        this.someCodeSet = someCodeSet;
    }
}