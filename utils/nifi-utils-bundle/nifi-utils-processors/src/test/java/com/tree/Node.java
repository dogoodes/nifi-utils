package com.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Apple on 4/22/17.
 */
public class Node<T> {

    private List<Node<T>> children = new ArrayList<>();
    private Node<T> parent = null;
    private T data = null;

    public Node(T data) {
        this.data = data;
    }

    public Node(T data, Node<T> parent) {
        this.data = data;
        this.parent = parent;
    }

    public List<Node<T>> getChildren() {
        return children;
    }

    public void setParent(Node<T> parent) {
        this.parent = parent;
    }

    public void addChild(T data) {
        Node<T> child = new Node<T>(data);
        child.setParent(this);
        this.children.add(child);
    }

    public void addChild(Node<T> child) {
        child.setParent(this);
        this.children.add(child);
    }

    public boolean hasChildren() {
        return !(children==null || children.size()==0);
    }

    public T getData() {
        return this.data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public boolean isRoot() {
        return (this.parent == null);
    }

    public boolean isLeaf() {
        if(this.children.size() == 0)
            return true;
        else
            return false;
    }

    public Node<T> getParent() {
        return parent;
    }

    public void removeParent() {
        this.parent = null;
    }

}