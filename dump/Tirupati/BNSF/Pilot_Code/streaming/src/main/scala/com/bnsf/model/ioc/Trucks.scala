package com.bnsf.model.ioc
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.fasterxml.jackson.annotation._

@JsonIgnoreProperties(ignoreUnknown = true)
class Trucks {

  @BeanProperty
  var `type`: String = _

  @BeanProperty
  var locationCode: String = _

  @BeanProperty
  var measurements: Array[Measurements] = _

  @BeanProperty
  var trainSide: String = _

  @BeanProperty
  var trainAxlePosition: Short = _
  @BeanProperty
  var trainEquipmentPosition: Short = _

  @BeanProperty
  var truckCode: String = _

  @BeanProperty
  var axleCode: String = _

  @BeanProperty
  var equipmentSide: String = _

  @BeanProperty
  var trainTruckPosition: Short = _

  var typeInfo: String = `type`

}
