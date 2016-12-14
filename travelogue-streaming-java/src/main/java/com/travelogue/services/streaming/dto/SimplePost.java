package com.travelogue.services.streaming.dto;

import java.io.Serializable;
import java.util.Arrays;

public class SimplePost implements Serializable {

	private static final long serialVersionUID = 6330501718240232742L;

	private Integer id;

	private String[] tags;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String[] getTags() {
		return tags;
	}

	public void setTags(String[] tags) {
		this.tags = tags;
	}

	@Override
	public String toString() {
		return "SimplePost [id=" + id + ", tags=" + Arrays.toString(tags) + "]";
	}

}
