
package com.demo.hadoop.common.hive.protocol;

/**
 * Description of Sql statement in XML file.
 */
public class SqlTypeInfo {
	private String theme;
	private String sql;
	private String date;
	private String type;
	private String name;
	private String frequency;
	private int limit;

	public String getTheme() {
		return theme;
	}

	public void setTheme(String theme) {
		this.theme = theme;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFrequency() {
		return frequency;
	}

	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	@Override
	public String toString() {
		return "SqlTypeInfo [theme=" + theme + ", sql=" + sql + ", date=" + date + ", type=" + type + ", name=" + name + ", frequency=" + frequency + ", limit=" + limit + "]";
	}

}
