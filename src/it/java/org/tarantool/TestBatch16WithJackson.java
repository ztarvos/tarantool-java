package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.tarantool.generic.TarantoolGenericBatchConnection16;
import org.tarantool.generic.TarantoolGenericBatchConnection16Impl;
import org.tarantool.schema.SchemaResolver;

public class TestBatch16WithJackson {
    /*
     Before executing this test you should configure your local tarantool

     box.cfg{listen=3301}
     box.schema.space.create('tester2')
     box.space.tester2:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

     box.schema.user.create('test', { password = 'test' })
     box.schema.user.grant('test', 'execute,read,write', 'universe')
     box.space['tester2']:format{{name='id', type='num'},{name='age', type='num'},{name='name', type='str'},{name='male', type='str'},{name='tags', type='array'},{name='links',type='array'}}


    */




    public static void main(String[] args) throws IOException {
        TarantoolGenericBatchConnection16 con = new TarantoolGenericBatchConnection16Impl(SocketChannel.open(new InetSocketAddress("localhost",3301)), new JacksonMapper());
        con.auth("test", "test");

        TarantoolConnection16 c = new TarantoolConnection16Impl("localhost", 3301);
        c.auth("test", "test");

        final TestSchema2 schema = new SchemaResolver().schema(new TestSchema2(), c);
        c.close();

        System.out.println(schema);
        con.begin();
        TarantoolGenericBatchConnection16.Holder<Pojo[]> delete = con.delete(Pojo[].class, schema.tester2.id, Arrays.asList(1));


        TarantoolGenericBatchConnection16.Holder<Pojo[]> insert = con.insert(Pojo[].class, schema.tester2.id, new Pojo());

        TarantoolGenericBatchConnection16.Holder<Pojo[]> select0 = con.select(Pojo[].class, schema.tester2.id, schema.tester2.primary, Arrays.asList(1), 0, 100, 0);

        TarantoolGenericBatchConnection16.Holder<Pojo[]> update0 = con.update(Pojo[].class, schema.tester2.id, Arrays.asList(1), Arrays.asList("=", 1, 66));

        TarantoolGenericBatchConnection16.Holder<Pojo[]> eval = con.eval(Pojo[].class, "return {age=99}"); //age should be overriden


        TarantoolGenericBatchConnection16.Holder<Pojo> eval2 = con.eval(Pojo.class,"return 1,99,'hello',false,{'no'},{4,5,6}");

        TarantoolGenericBatchConnection16.Holder<Pojo[]> eval3= con.eval(Pojo[].class,"return {1,99,'hello',false,{'no'},{4,5,6}}");

        con.end();

        con.get();

        System.out.println(Arrays.toString(delete.get()));
        System.out.println(Arrays.toString(insert.get()));
        System.out.println(Arrays.toString(select0.get()));
        System.out.println(Arrays.toString(update0.get()));
        System.out.println(Arrays.toString(eval.get()));
        System.out.println(eval2);

        System.out.println(Arrays.toString(eval3.get()));
        con.close();
    }
}
