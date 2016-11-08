package com.nifi.processors.crud;

public class ConversaoPhpJava implements Database {
	
//	$this->queries = array(
//			private static final String list_tables = 'SELECT "TABLE_NAME","TABLE_COMMENT" FROM "INFORMATION_SCHEMA"."TABLES" WHERE "TABLE_SCHEMA" = ?',
//			
//			'reflect_table'=>'SELECT
//					"TABLE_NAME"
//				FROM
//					"INFORMATION_SCHEMA"."TABLES"
//				WHERE
//					"TABLE_NAME" COLLATE \'utf8_bin\' = ? AND
//					"TABLE_SCHEMA" = ?',
//			'reflect_pk'=>'SELECT
//					"COLUMN_NAME"
//				FROM
//					"INFORMATION_SCHEMA"."COLUMNS"
//				WHERE
//					"COLUMN_KEY" = \'PRI\' AND
//					"TABLE_NAME" = ? AND
//					"TABLE_SCHEMA" = ?',
//			'reflect_belongs_to'=>'SELECT
//					"TABLE_NAME","COLUMN_NAME",
//					"REFERENCED_TABLE_NAME","REFERENCED_COLUMN_NAME"
//				FROM
//					"INFORMATION_SCHEMA"."KEY_COLUMN_USAGE"
//				WHERE
//					"TABLE_NAME" COLLATE \'utf8_bin\' = ? AND
//					"REFERENCED_TABLE_NAME" COLLATE \'utf8_bin\' IN ? AND
//					"TABLE_SCHEMA" = ? AND
//					"REFERENCED_TABLE_SCHEMA" = ?',
//			'reflect_has_many'=>'SELECT
//					"TABLE_NAME","COLUMN_NAME",
//					"REFERENCED_TABLE_NAME","REFERENCED_COLUMN_NAME"
//				FROM
//					"INFORMATION_SCHEMA"."KEY_COLUMN_USAGE"
//				WHERE
//					"TABLE_NAME" COLLATE \'utf8_bin\' IN ? AND
//					"REFERENCED_TABLE_NAME" COLLATE \'utf8_bin\' = ? AND
//					"TABLE_SCHEMA" = ? AND
//					"REFERENCED_TABLE_SCHEMA" = ?',
//			'reflect_habtm'=>'SELECT
//					k1."TABLE_NAME", k1."COLUMN_NAME",
//					k1."REFERENCED_TABLE_NAME", k1."REFERENCED_COLUMN_NAME",
//					k2."TABLE_NAME", k2."COLUMN_NAME",
//					k2."REFERENCED_TABLE_NAME", k2."REFERENCED_COLUMN_NAME"
//				FROM
//					"INFORMATION_SCHEMA"."KEY_COLUMN_USAGE" k1,
//					"INFORMATION_SCHEMA"."KEY_COLUMN_USAGE" k2
//				WHERE
//					k1."TABLE_SCHEMA" = ? AND
//					k2."TABLE_SCHEMA" = ? AND
//					k1."REFERENCED_TABLE_SCHEMA" = ? AND
//					k2."REFERENCED_TABLE_SCHEMA" = ? AND
//					k1."TABLE_NAME" COLLATE \'utf8_bin\' = k2."TABLE_NAME" COLLATE \'utf8_bin\' AND
//					k1."REFERENCED_TABLE_NAME" COLLATE \'utf8_bin\' = ? AND
//					k2."REFERENCED_TABLE_NAME" COLLATE \'utf8_bin\' IN ?'

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
	
	//TODO
//	public function fetchAssoc($result) {
//		return mysqli_fetch_assoc($result);
//	}
	
	//TODO
//	public function fetchRow($result) {
//		return mysqli_fetch_row($result);
//	}
	
	//TODO
//	public function insertId($result) {
//		return mysqli_insert_id($this->db);
//	}
	
	//TODO
//	public function affectedRows($result) {
//		return mysqli_affected_rows($this->db);
//	}

	//TODO
//	public function close($result) {
//		return mysqli_free_result($result);
//	}
	
	//TODO
//	public function fetchFields($table) {
//		$result = $this->query('SELECT * FROM ! WHERE 1=2;',array($table));
//		return mysqli_fetch_fields($result);
//	}
	
//	public String addLimitToSql(String sql, String limit, String offset) {
//		return sql + " LIMIT " +  limit + " OFFSET "  + offset;
//	}

	@Override
	public String getSql(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String query(String sql, String params) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String addLimitToSql(String sql, String limit, String offset) {
		return sql + " LIMIT " +  limit + " OFFSET "  + offset;
	}
	
	//TODO
//	public String likeEscape(String string) {
//		return addcslashes(string,'%_');
//	}

}
