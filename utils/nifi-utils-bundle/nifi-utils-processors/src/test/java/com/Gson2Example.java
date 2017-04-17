package com;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.nifi.processors.utils.FlowFileStructure;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

public class Gson2Example {

    public static void main(String[] args) {

        Gson gson = new Gson();
        Map<String, String> attributes = new HashMap<String, String>();

        try (Reader reader = new FileReader("/Volumes/Fabiano 1/developer/testes/json/filecomplete.json")) {

			//1. Convert JSON to Java Object
//            Staff staff = gson.fromJson(reader, Staff.class);
//            System.out.println(staff.toString());

			//2. Convert JSON to JsonElement, and later to String
//            JsonElement json = gson.fromJson(reader, JsonElement.class);
//            String jsonInString = gson.toJson(json);
//            System.out.println(jsonInString);

            //3.Convert JSON to Map
//            final Gson gson = new Gson();
//            attributes = gson.fromJson(json, new TypeToken<Map<JsonElement, JsonElement>>(){}.getType());
//            System.out.println(attributes);

            //4 Convert JSON to POJO
            FlowFileStructure ffs = gson.fromJson(reader, FlowFileStructure.class);
            attributes = gson.fromJson(ffs.getAttribute(), new TypeToken<Map<String, String>>(){}.getType());
            System.out.println();


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
