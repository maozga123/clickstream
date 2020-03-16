package com.clickstream.stat.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClickStreamRow implements Serializable
{
	Integer	 client_id;
	String user_id;
	Integer account_id;
	String activity;
	String module;

}
