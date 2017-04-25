package com.tree;

import java.util.HashMap;
import java.util.Map;

public class TestTree<S> {

    public static void main(String[] args) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("person.name", "Fabiano");
        attributes.put("person.lastname", "Matias");
        attributes.put("person.address.name", "Via transversal sul");
        attributes.put("person.address.number", "200");
        attributes.put("person.address.postalCode", "11111-111");

        MyTree<String> tree = new MyTree<String>("person");
        TreeNode<String> root = tree.getRoot();
        System.out.println("Root: " + root);

        for(final Map.Entry<String, String> attribute : attributes.entrySet()) {
            String[] attr = attribute.getKey().split("\\.");

            TreeNode<String> node;

            for (int i=0; i<attr.length; i++) {
                String word = attr[i];
                recursividade(word, root);
            }
        }
    }

    public static TreeNode<String> recursividade(String word, TreeNode<String> parent) {
        if (parent.getParent().equals(word))
            return recursividade(word, parent);

        return parent.addChild(word);
    }

//    private TreeNode<String> prepareTree() {
//        TreeNode<String> treeNodeToGet = null;
//        List<TreeNode<String>> children = tree.getRoot().getChildren();
//        for (TreeNode<String> treeNode : children) {
//            if (treeNode != null) {
//                System.out.println(treeNode.getElement());
//                List<TreeNode<String>> children2 = treeNode.getChildren();
//                if (treeNode.getChildren() != null) {
//                    for (TreeNode<String> treeNode2 : children2) {
//                        if (treeNode2 != null) {
//                            System.out.println(treeNode2.getElement() + "("
//                                    + treeNode2.getParent().getElement() + ")");
//                            treeNodeToGet = treeNode2;
//                        }
//                    }
//                }
//            }
//        }
//        return treeNodeToGet;
//    }


//    for(final Map.Entry<String, String> attribute : attributes.entrySet()) {
//        String[] attr = attribute.getKey().split(".");
//
//        MyTree<String> tree = new MyTree<String>("person");
//        TreeNode<String> root = tree.getRoot();
//        System.out.println("Root: " + root);
//
//        TreeNode<String> node;
//        List<TreeNode<String>> children = tree.getRoot().getChildren();
//
//        for (int i=0; i<attr.length; i++) {
//            if (attr[i].equals(root)) {
//                //Não faz nada, se a primeira palavra for o root
//                System.out.println("o valor é o root, não precisa adicionar");
//                return;
//            }
//
//            //TODO Dá p melhorar bastante, já que o root pode ser considerado parent de todos
//
//            //Se o root não tem filho, adiciona o filho
//            if (!root.hasChildren()) {
//                root.addChild(attr[i]);
//                System.out.println("Primeiro filho do root: " + attr[i]);
//            }
//            else {
//                //Se o filho do root já existe
//                if (root.getChildren().equals(attr[i])) {
//                    //
//                }
//            }
//        }
//    }

}