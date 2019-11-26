package com.bnsf.model.ioc
import scala.beans.{ BeanProperty, BooleanBeanProperty }

import com.fasterxml.jackson.annotation._
import java.sql.Timestamp

@JsonIgnoreProperties(ignoreUnknown = true)
class Header {

  @BeanProperty
  var receiveTime: String = _

  @BeanProperty
  var messageId: Long = _

}

