package com.bnsf.model.ioc
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.fasterxml.jackson.annotation._

@JsonIgnoreProperties(ignoreUnknown = true)
class Message {

  @BeanProperty
  var body: Body = _

  @BeanProperty
  var header: Header = _

  @BeanProperty
  var test: String = _

  override def toString(): String =
    "ClassPojo [body = " + body + ", header = " + header +
      "]"

}
