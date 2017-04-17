package com;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.nifi.processors.utils.FlowFileStructure;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

/**
 * Created by Apple on 4/15/17.
 */
public class TestFlowFileStructure {

    public static void main(String[] args) throws FileNotFoundException {
        Reader reader = new FileReader("/Volumes/Fabiano 1/developer/testes/json/filecomplete.json");

        final Gson gson = new GsonBuilder().create();
        JsonElement jsonContent = gson.fromJson(reader, JsonElement.class);

        FlowFileStructure flowFileStructure =
                new FlowFileStructure.Builder()
                        .attribute(null)
                        .content(jsonContent)
                        .build();

        String json = gson.toJson(flowFileStructure);
        System.out.println(json);
    }

}