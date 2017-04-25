package com.tree;

import java.util.ArrayList;
import java.util.List;

public class TreeNode<T> {
	
		private T element;
		private TreeNode<T> parent;
		private List<TreeNode<T>> children;
		
		public TreeNode(T element, TreeNode<T> parent) {
			this.element=element;
			this.parent=parent;
		}

		public TreeNode() {}

		public T getElement() {
			return element;
		}

		public void setElement(T element) {
			this.element = element;
		}

		public TreeNode<T> getParent() {
			return parent;
		}

		public void setParent(TreeNode<T> parent) {
			this.parent = parent;
		}

		public List<TreeNode<T>> getChildren() {
			return children;
		}

		public void setChildren(List<TreeNode<T>> children) {
			this.children = children;
		}
		
		public boolean hasChildren() {
			return children==null || children.size()==0;
		}
		
		public TreeNode<T> addChild(T element){
			if(children==null){
				children = new ArrayList<TreeNode<T>>();
			}
			
			TreeNode<T> treeNode = new TreeNode<T>();
			treeNode.setElement(element);
			treeNode.setParent(this);
			this.children.add(treeNode);
			return treeNode;
		}
		
		public void addChildNode(TreeNode<T> node){
			this.addChildNode(node);
		}

		@Override
		public String toString(){
			return this.getElement().toString();
		}

}