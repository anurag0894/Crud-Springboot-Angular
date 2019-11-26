package com.bnsf.model.ioc
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.fasterxml.jackson.annotation._
import java.sql.Timestamp
import java.sql.Date
@JsonIgnoreProperties(ignoreUnknown = true)
class Train {

  @BeanProperty
  var trainId: String = _

  @BeanProperty
  var trainMeasurementTime: String = _

  @BeanProperty
  var trainDepartureTime: String = _

  @BeanProperty
  var confidence: Double = _

  @BeanProperty
  var keyTrain: String = _

  @BeanProperty
  var hhftTrain: String = _

  @BeanProperty
  var measurements: Array[Measurements] = _

  @BeanProperty
  var equipments: Array[Equipments] = _

}
