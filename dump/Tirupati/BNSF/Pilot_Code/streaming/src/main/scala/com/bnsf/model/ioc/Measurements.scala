package com.bnsf.model.ioc
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.fasterxml.jackson.annotation._
import java.sql.Timestamp

@JsonIgnoreProperties(ignoreUnknown = true)
class Measurements {

  @BeanProperty
  var timestamp: String = _

  @BeanProperty
  var statusCode: String = _

  @BeanProperty
  var unitOfMeasure: String = _

  @BeanProperty
  var strValue: String = _

  @BeanProperty
  var measurementId: Long = _

  @BeanProperty
  var confidenceLevel: Double = _

  @BeanProperty
  var typeCode: String = _

  @BeanProperty
  var detectorCalibrationConfidenceLevel: Double = _

  @BeanProperty
  var measurementClass: String = _

  @BeanProperty
  var qualityCode: String = _

}
