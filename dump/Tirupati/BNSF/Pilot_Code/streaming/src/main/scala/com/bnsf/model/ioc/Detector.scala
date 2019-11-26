package com.bnsf.model.ioc
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.fasterxml.jackson.annotation._
@JsonIgnoreProperties(ignoreUnknown = true)
class Detector {

  @BeanProperty
  var aeiEnabledFlag: String = _

  @BeanProperty
  var detectorGroupId: Long = _

  @BeanProperty
  var measurements: Array[Measurements] = _

  @BeanProperty
  var detectorId: Int = _

}
