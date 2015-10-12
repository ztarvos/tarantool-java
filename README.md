# Java Connector for Tarantool 1.6

## How to start

First you should add maven repository and dependency to your pom file
```xml
<repository>
  <id>tarantool-java-repo</id>
  <name>Tarantool java connector repository</name>
  <url>https://cdn.rawgit.com/tarantool/tarantool-java/master/maven-repo/</url>
</repository>
```
```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.6.3-SNAPSHOT</version>
</dependency>
```
Afterward you should configure you tarantool and create any type of connection ([source code] (https://github.com/tarantool/tarantool-java/blob/master/src/it/java/org/tarantool/TestClient16.java)):
```java
public class TestClient16 {
    /*
      Before executing this test you should configure your local tarantool
      box.cfg{listen=3301}
      box.schema.space.create('tester')
      box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})
      box.schema.user.create('test', { password = 'test' })
      box.schema.user.grant('test', 'execute,read,write', 'universe')
     */
    public static void main(String[] args) throws IOException {
        TarantoolConnection16 con = new TarantoolConnection16Impl("localhost", 3301);
        con.auth("test", "test");

        final TestSchema schema = con.schema(new TestSchema());
        System.out.println(schema);

        List delete0 = con.delete(schema.tester.id, Arrays.asList(0));
        System.out.println(delete0);
        List delete = con.delete(schema.tester.id, Arrays.asList(1));
        System.out.println(delete);
        List insert = con.insert(schema.tester.id, Arrays.asList(1, "hello"));
        System.out.println(insert);
        List insert2 = con.replace(schema.tester.id, Arrays.asList(2, Collections.singletonMap("hello", "word"),new String[]{"a","b","c"}));
        System.out.println(insert2);
        List select0 = con.select(schema.tester.id, schema.tester.primary, Arrays.asList(1), 0, 100, 0);
        System.out.println(select0);
        List update0 = con.update(schema.tester.id, Arrays.asList(1), Arrays.asList("=", 1, "Hello"));
        System.out.println(update0);
        List result = con.call("math.ceil", 1.3);
        System.out.println(result);
        List eval = con.eval("return ...", 1, 2, 3);
        System.out.println(eval);
        con.close();
    }
}
```
We also provide more usable connection implementations:
* [pojo mapping example](https://github.com/tarantool/tarantool-java/blob/master/src/it/java/org/tarantool/TestClient16WithJackson.java)
* [async queries example](https://github.com/tarantool/tarantool-java/blob/master/src/it/java/org/tarantool/TestClient16Async.java) 
* [async queries with pojo mapping example](https://github.com/tarantool/tarantool-java/blob/master/src/it/java/org/tarantool/TestClient16AsyncWithJackson.java) 
* [batch mode example](https://github.com/tarantool/tarantool-java/blob/master/src/it/java/org/tarantool/TestBatch16.java) 
* [batch mode with pojo mapping example](https://github.com/tarantool/tarantool-java/blob/master/src/it/java/org/tarantool/TestClient16Async.java) 


