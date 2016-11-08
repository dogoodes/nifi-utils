package com.nifi.processors.crud;

public interface Database {
	
	String getSql(String name);
	
	//TODO Esse será o DBCP
//	public function connect($hostname,$username,$password,$database,$port,$socket,$charset);
	
	String query(String sql, String params);
	
//	function fetchAssoc($result);
//	
//	function fetchRow($result);
//	
//	function insertId($result);
//	
//	function affectedRows($result);
//	
//	function close($result);
//	
//	function fetchFields($table);
	
	String addLimitToSql(String sql, String limit, String offset);
	
//	function likeEscape($string);
//	
//	function isBinaryType($field);
//	
//	function isGeometryType($field);
//	
//	function getDefaultCharset();

}