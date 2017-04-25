package com.tree;

import java.util.List;


public class MyTree<T> {
	
	private TreeNode<T> root = null;

	public MyTree(){}
	
	public MyTree(T rootElement) {
		root = new TreeNode<T>(rootElement,null);
	}
	
	public void addElementAt(T element,TreeNode<T> node){
		node.addChild(element);
	}

	public T getRootElement(){
		return root.getElement();
	}
	
	public void setRootElement(T element){
		root.setElement(element);
	}
	
	public TreeNode<T> getRoot() {
		return root;
	}

	public void setRoot(TreeNode<T> root) {
		this.root = root;
	}

	public boolean isRoot(TreeNode<T> node){
		return node==root;
	}
	
	public boolean isExternal(TreeNode<T> node){
		return node.hasChildren();
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for (TreeNode<T> node : root.getChildren()) {
			sb.append(node.toString()+"--");
		}
		return sb.toString();
	}
	
	public int depth(TreeNode<T> node){
		if(isRoot(node)){
			return 0;
		}else{
			return 1+depth(node.getParent());
		}
	}
	
	public int height(TreeNode<T> node){
		if(isExternal(node)){
			return 0;
		}else{
			int h =0;
			
			for (TreeNode<T> childNode : node.getChildren()) {
				h = Math.max(h, height(childNode));
			}
			return 1+h;
		}
		
	}
	
	public String preOrderTraversal(TreeNode<T> node) {
		StringBuilder sb = new StringBuilder();
		sb.append(node.toString() + ", ");
		List<TreeNode<T>> children = node.getChildren();
		if (children != null) {
			for (TreeNode<T> treeNode : children) {
				sb.append(preOrderTraversal(treeNode));
			}
		}
		return sb.toString();
	}

	public String postOrder(TreeNode<T> node){
		StringBuilder sb = new StringBuilder();
		
		List<TreeNode<T>> children = node.getChildren();
		if (children != null) {
			for (TreeNode<T> treeNode : children) {
				sb.append(postOrder(treeNode));
			}
		}
		sb.append(node.toString() + ", ");
		return sb.toString();
	}

}
	
