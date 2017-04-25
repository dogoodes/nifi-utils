package com;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.nifi.processors.utils.FlowFileStructure;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TesteEntityMapper {

//    public static void main(String[] args) {
//        Gson gson = new Gson();
//        Map<String, String> attributes = new HashMap<String, String>();
//
//        try (Reader reader = new FileReader("/Volumes/Fabiano 2/developer/testes/json/filecomplete.json")) {
//
//            JsonElement je = gson.fromJson(reader, JsonElement.class);
//            System.out.println();
//            JsonObject jo = je.getAsJsonObject();
//
//
//
////            JsonElement je = gson.toJsonTree(reader);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static void main(String[] args) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("person.name", "Fabiano");
        attributes.put("person.lastname", "Matias");
        attributes.put("person.address.name", "Via transversal sul");
        attributes.put("person.address.number", "200");
        attributes.put("person.address.postalCode", "11111-111");

        for(final Map.Entry<String, String> attribute : attributes.entrySet()){
            String[] attr = attribute.getKey().split(".");

            for (int i=0; i<attr.length; i++) {

            }

        }


//        Node<String> parentNode = new Node<String>("Parent");
//        Node<String> childNode1 = new Node<String>("Child 1", parentNode);
//        Node<String> childNode2 = new Node<String>("Child 2");
//
//        childNode2.setParent(parentNode);
//
//        Node<String> grandchildNode = new Node<String>("Grandchild of parentNode. Child of childNode1", childNode1);
//        List<Node<String>> childrenNodes = parentNode.getChildren();
    }

}
