
package com.bnsf.model.ioc
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.fasterxml.jackson.annotation._

//remove if not needed
import scala.collection.JavaConversions._
@JsonIgnoreProperties(ignoreUnknown = true)
class Body {

  @BeanProperty
  var messageQualityCode: String = _

  @BeanProperty
  var train: Train = _

  @BeanProperty
  var messageAEIStatus: String = _

  @BeanProperty
  var detector: Detector = _

}
