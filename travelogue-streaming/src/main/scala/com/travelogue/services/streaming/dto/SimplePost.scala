package com.travelogue.services.streaming.dto

class SimplePost(id: Int, tags: Array[String]) {
  def this() {
    this(0, new Array[String](0))
  }

  override def toString() = {
    "SimplePost [id=" + id + ", tags=" + tags + "]"
  }
}