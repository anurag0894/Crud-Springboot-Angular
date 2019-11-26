package com.bnsf.model.ioc

import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.fasterxml.jackson.annotation._
import java.sql.Timestamp
import java.sql.Date

@JsonIgnoreProperties(ignoreUnknown = true)
class Equipments {

  @BeanProperty
  var position: Short = _

  @BeanProperty
  var measurements: Array[Measurements] = _

  @BeanProperty
  var wheels: Array[Wheels] = _

  @BeanProperty
  var trucks: Array[Trucks] = _

  @BeanProperty
  var axles: Array[Axles] = _

  @BeanProperty
  var number: String = _

  @BeanProperty
  var `type`: String = _

  @BeanProperty
  var initial: String = _

  @BeanProperty
  var orientation: String = _

  @BeanProperty
  var configurationType: String = _

  @BeanProperty
  var equipmentMatchConfidenceLevel: Double = _

  @BeanProperty
  var grossRailWeight: Int = _

  @BeanProperty
  var buildDate:String = _

  @BeanProperty
  var umlerTypeCode: String = _

  @BeanProperty
  var hazmatEquipment: String = _

  @BeanProperty
  var stccNumber: String = _

  @BeanProperty
  var commodityDesc: String = _

  @BeanProperty
  var bnsfTypeCode: String = _

  @BeanProperty
  var draftGearTypeCode: String = _

  @BeanProperty
  var loadEmptyCode: String = _

  @BeanProperty
  var startTrainAxlePosition: Short = _

  @BeanProperty
  var endTrainAxlePosition: Short = _

}
