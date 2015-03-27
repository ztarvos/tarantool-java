package org.tarantool;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "age", "name", "male", "tags", "links"})
public class Pojo {
    int id = 1;
    String name = "john smith";
    int age = 12;
    boolean male = true;
    String[] tags = new String[]{"a", "b", "c"};
    List<Integer> links = Arrays.asList(1, 2, 3);

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public boolean isMale() {
        return male;
    }

    public String[] getTags() {
        return tags;
    }

    public List<Integer> getLinks() {
        return links;
    }

    @Override
    public String toString() {
        return "Pojo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", male=" + male +
                ", tags=" + Arrays.toString(tags) +
                ", links=" + links +
                '}';
    }
}