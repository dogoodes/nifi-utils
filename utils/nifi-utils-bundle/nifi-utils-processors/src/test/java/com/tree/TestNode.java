package com.tree;

import java.util.HashMap;
import java.util.Map;

public class TestNode<S> {

    public static void main(String[] args) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("user.person.name", "Fabiano");
        attributes.put("user.person.lastname", "Matias");
        attributes.put("user.person.address.name", "Via transversal sul");
        attributes.put("user.person.address.number", "200");
        attributes.put("user.person.address.postalCode", "11111-111");

        Node<String> root = new Node<String>("user");

        for(final Map.Entry<String, String> attribute : attributes.entrySet()) {
            String[] attr = attribute.getKey().split("\\.");
            String value = root.getData();
            Node<String> node = root;
            Node<String> child;
            for (int i=0; i<attr.length; i++) {
                if(!attr[i].equalsIgnoreCase(root.getData())) {
                    value += "."+attr[i];
                    node = associate(node, value, true);
                }
            }
        }

        String json = generateJson(root, attributes, root);
        System.out.println(json);
    }

    private static Node<String> associate(Node<String> root, String value, boolean isRoot) {
        Node<String> child = null;
        if(!root.hasChildren() && isRoot) {
            child = new Node<String>(value);
            root.addChild(child);
            return child;
        } else {
            for (Node<String> node : root.getChildren()) {
                if(node.getData().equalsIgnoreCase(value)) {
                    return node;
                }
            }
            for (Node<String> node : root.getChildren()) {
                child = associate(node, value, false);
                if(child != null) {
                    return child;
                }
            }
        }

        if(isRoot) {
            child = new Node<String>(value);
            root.addChild(child);
        }
        return child;
    }

    private static String generateJson(Node<String> node, Map<String, String> attributes, Node<String> root) {
        StringBuilder json = new StringBuilder();
        Node<String> child;
        String[] data = node.getData().split("\\.");

        if (node.getData().equalsIgnoreCase(root.getData()))
            json.append("{");

        json.append("\"");
        json.append(data[data.length - 1]);
        json.append("\"");
        json.append(":");
        if(node.hasChildren()) {
            json.append("{");
            for (int i = 0; i < node.getChildren().size(); i++) {
                child = node.getChildren().get(i);
                json.append(generateJson(child, attributes, root));
                if(node.getChildren().size() > 1 && i < node.getChildren().size() - 1) {
                    json.append(",");
                }
            }
            json.append("}");
        } else {
            json.append("\"");
            json.append(attributes.get(node.getData()));
            json.append("\"");
        }

        if (node.getData().equalsIgnoreCase(root.getData()))
            json.append("}");

        return json.toString();
    }

}