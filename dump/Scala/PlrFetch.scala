import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.broadcast.Broadcast
import java.lang.Math

object PlrFetch
{

//Function to Broadcast Columns from Catalog List needed for PRODUCT PLATFORM and PRODUCT GROUP as HashMap
def broadCastPlat (spark:SparkSession): Broadcast[Map[String,(String, String,String)]] = {
val sc = spark.sparkContext
import spark.implicits._
val df_tbl_catalog_list_plant = spark.sql(""" select max(PRODUCT_PLATFORM) as PRODUCT_PLATFORM,max(SUB_PRODUCT_LINE) as SUB_PRODUCT_LINE,max(PRODUCT_GROUP) as PRODUCT_GROUP, concat(trim(item_number),',',trim(PLANTCODE)) as key from g00103.IQP_LEGACY_CATALOG_LIST group by item_number,PLANTCODE""")
val lookHashItemOrg = df_tbl_catalog_list_plant.map(x =>  (x.getAs[String]("key"),(x.getAs[String]("PRODUCT_PLATFORM"),x.getAs[String]("PRODUCT_GROUP"),x.getAs[String]("SUB_PRODUCT_LINE")))).rdd.collectAsMap().toMap

val df_tbl_catalog_list_platform = spark.sql(""" select max(PRODUCT_PLATFORM) as PRODUCT_PLATFORM,max(SUB_PRODUCT_LINE) as SUB_PRODUCT_LINE, max(PRODUCT_GROUP) as PRODUCT_GROUP, trim(item_number) as item_number from g00103.IQP_LEGACY_CATALOG_LIST group by item_number""")
val lookHashItem  = df_tbl_catalog_list_platform.map(x =>  (x.getAs[String]("item_number"),(x.getAs[String]("PRODUCT_PLATFORM"),x.getAs[String]("PRODUCT_GROUP"),x.getAs[String]("SUB_PRODUCT_LINE")))).rdd.collectAsMap().toMap

val df_tbl_catalog_prod_class = spark.sql(""" select max(PRODUCT_PLATFORM) as PRODUCT_PLATFORM,max(SUB_PRODUCT_LINE) as SUB_PRODUCT_LINE, max(PRODUCT_GROUP) AS PRODUCT_GROUP, PRODUCTCLASS  from g00103.IQP_LEGACY_CATALOG_LIST group by PRODUCTCLASS""")
val lookHashPrdClass = df_tbl_catalog_prod_class.map(x =>  (x.getAs[String]("PRODUCTCLASS"),(x.getAs[String]("PRODUCT_PLATFORM"),x.getAs[String]("PRODUCT_GROUP"),x.getAs[String]("SUB_PRODUCT_LINE")))).rdd.collectAsMap().toMap

val df_tbl_catalog_prod_group = spark.sql(""" select  max(PRODUCT_PLATFORM) as PRODUCT_PLATFORM,max(SUB_PRODUCT_LINE) as SUB_PRODUCT_LINE, max(PRODUCT_GROUP) AS PRODUCT_GROUP, PRODUCT_GROUP from g00103.IQP_LEGACY_CATALOG_LIST group by PRODUCT_GROUP""")
val lookHashPrdGroup = df_tbl_catalog_prod_group.map(x =>  (x.getAs[String]("PRODUCT_GROUP"),(x.getAs[String]("PRODUCT_PLATFORM"),x.getAs[String]("PRODUCT_GROUP"),x.getAs[String]("SUB_PRODUCT_LINE")))).rdd.collectAsMap().toMap

val FinalHash = lookHashItemOrg ++ lookHashItem  ++ lookHashPrdClass ++ lookHashPrdGroup
val broadcastProductPlatform = sc.broadcast(FinalHash)

broadcastProductPlatform 
}

//Function to Broadcast Columns from Catalog List needed for PRODUCT SEGMENT and PRODUCT LINE as HashMap
def broadCastCat (spark:SparkSession): Broadcast[Map[String,(String, String,String)]] = {
val sc = spark.sparkContext
import spark.implicits._
val df_tbl_catalog_list_plant = spark.sql(""" select max(productline) as product_line,max(product_segment) as product_segment,max(productclass) as product_class, concat(trim(item_number),',',trim(PLANTCODE)) as key from g00103.IQP_LEGACY_CATALOG_LIST group by item_number,PLANTCODE""")
val lookHashItemOrg = df_tbl_catalog_list_plant.map(x =>  (x.getAs[String]("key"),(x.getAs[String]("product_line"),x.getAs[String]("product_segment"),x.getAs[String]("product_class")))).rdd.collectAsMap().toMap

val df_tbl_catalog_list_platform = spark.sql(""" select max(productline) as product_line,max(product_segment) as product_segment, max(productclass) as product_class, trim(item_number) as item_number from g00103.IQP_LEGACY_CATALOG_LIST group by item_number""")
val lookHashItem  = df_tbl_catalog_list_platform.map(x =>  (x.getAs[String]("item_number"),(x.getAs[String]("product_line"),x.getAs[String]("product_segment"),x.getAs[String]("product_class")))).rdd.collectAsMap().toMap

val df_tbl_catalog_prod_class = spark.sql(""" select max(productline) as productline,max(product_segment) as product_segment, PRODUCTCLASS  from g00103.IQP_LEGACY_CATALOG_LIST group by PRODUCTCLASS""")
val lookHashPrdClass = df_tbl_catalog_prod_class.map(x =>  (x.getAs[String]("PRODUCTCLASS"),(x.getAs[String]("productline"),x.getAs[String]("product_segment"),x.getAs[String]("PRODUCTCLASS")))).rdd.collectAsMap().toMap

val df_tbl_catalog_prod_group = spark.sql(""" select  max(productline) as productline,max(product_segment) as product_segment, max(productclass) AS productclass, PRODUCT_GROUP from g00103.IQP_LEGACY_CATALOG_LIST group by PRODUCT_GROUP""")
val lookHashPrdGroup = df_tbl_catalog_prod_group.map(x =>  (x.getAs[String]("PRODUCT_GROUP"),(x.getAs[String]("productline"),x.getAs[String]("product_segment"),x.getAs[String]("productclass")))).rdd.collectAsMap().toMap

val FinalHash1 = lookHashItemOrg ++ lookHashItem  ++ lookHashPrdClass ++ lookHashPrdGroup
val broadcastCatalog = sc.broadcast(FinalHash1)

broadcastCatalog 
}

//Function to Broadcast Columns from IQP_SUB_BUSINESS table needed for Business_segment as HashMap
def broadCastSubBus (spark:SparkSession):Broadcast[Map[(String,String),String]] = {
val sc = spark.sparkContext
import spark.implicits._
val df_tbl_sub_business = spark.sql(""" select max(sub_business) as sub_business,product_platform,pnl  from g00103.iqp_sub_business_dim group by product_platform,pnl """)
val lookHashSubBus = df_tbl_sub_business.map(x =>  ((x.getAs[String]("product_platform"),x.getAs[String]("pnl")),x.getAs[String]("sub_business"))).rdd.collectAsMap().toMap 
val broadcastSubBusiness = sc.broadcast(lookHashSubBus)

broadcastSubBusiness 
}

//Function to Broadcast Columns from Catalog List needed for Productclass, Management_entity as HashMap
def broadCastMEClass (spark:SparkSession): Broadcast[Map[String,(String, String,String)]] = {
val sc = spark.sparkContext
import spark.implicits._
val df_tbl_catalog_list_plant = spark.sql(""" select max(productclass) as product_class,max(product_segment) as product_segment,max(productclass) as product_class, concat(trim(item_number),',',trim(PLANTCODE)) as key from g00103.IQP_LEGACY_CATALOG_LIST group by item_number,PLANTCODE""")
val lookHashItemOrg = df_tbl_catalog_list_plant.map(x =>  (x.getAs[String]("key"),(x.getAs[String]("product_class"),x.getAs[String]("product_segment"),x.getAs[String]("product_class")))).rdd.collectAsMap().toMap

val df_tbl_catalog_list_platform = spark.sql(""" select max(productclass) as product_class,max(product_segment) as product_segment, max(productclass) as product_class, trim(item_number) as item_number from g00103.IQP_LEGACY_CATALOG_LIST group by item_number""")
val lookHashItem  = df_tbl_catalog_list_platform.map(x =>  (x.getAs[String]("item_number"),(x.getAs[String]("product_class"),x.getAs[String]("product_segment"),x.getAs[String]("product_class")))).rdd.collectAsMap().toMap

val FinalHash1 = lookHashItemOrg ++ lookHashItem  
val broadcastMEClass = sc.broadcast(FinalHash1)

broadcastMEClass 
}

//FUnction definition for Product_platform (getProductPlatform is main function)
def getProductPlat(broadcastProductPlatform:Broadcast[Map[String,(String, String,String)]],broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getProductPlatform = (tier2:String,tier3:String,productNumber:String,item_id:String,organization_Id:String,pnl:String,subpnl:String,organiz_unit:String,management_entity:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("0").toDouble.toInt.toString.trim
val Organ_unit = Option(organiz_unit).getOrElse("").trim.toUpperCase
val ME = Option(management_entity).getOrElse("").trim.toUpperCase
val product_platform_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._1
val product_group_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._2
val sub_product_line_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._3
val product_segment_plr = broadcastCatalog.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastCatalog.value.getOrElse(productNumberO,("","","")))._2

val product_platform_ga = 
    if (product_platform_plr == null || product_platform_plr == "" ) {
        if(tier2 == "GRID AUTOMATION" && tier3 == "M&D") sub_product_line_plr
        else if(tier3 == "PROTECTION & CONTROL" )
        {   if(productNumberO.contains("-") )  "GATEWAYS/RTUS/SAS" else "Accessories"  } 
      else "TBD" }
    else product_platform_plr.toString
   
val product_platform_pd = 
    if (pnl == "ANASCO") product_platform_plr
   else if(pnl =="CLEARWATER" || pnl =="SOWO") product_segment_plr
   else if(tier2 == "CAPACITORS" && tier3 == "CAPACITORS") product_platform_plr
   else if(tier2 == "CAPACITORS" && tier3 == "NETWORK REGS & TRANS") subpnl
   else if(tier2 == "ACS" ) subpnl
   else if(tier2 == "XD PRIMARY EQUIPMENT") "XD PRIMARY EQUIPMENT"
   else subpnl
  
val product_platform_others = if( ! Array("IO MULTILIN CA","IO MULTILIN PR","IO MULTILIN US","IO CLEARWATER", "IO FT.EDWARD", "IO MT.JULIET","IO LARGO", "IO ROCHESTER").contains(Organ_unit)) "T&D Products"
                       else product_platform_plr

val result = if(ME.contains("MDS")) "PROJECT"
         else if(Array("J700", "J70S", "J74I","J74J","J7P","J7PA","J7PS","J74PA","CAPJ").contains(ME)) "AUTOMATION PROJECTS"
         else if(ME == "USPJ") "IEMS"
         else if(Array("SPUK", "J74H", "CAPK", "USPK").contains(ME)) "PACKAGED SOLUTIONS"
         else if(Array("5750", "J74H", "DG14", "DG24").contains(ME)) "T&D PRODUCTS"
            else  if (Array("XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS").contains(Option(tier2).getOrElse("")))  product_platform_pd
             else if (tier2 == "GRID AUTOMATION") product_platform_ga  
             else product_platform_others             
result
}
getProductPlatform  
}

//FUnction definition for Product_group (getProductGroup is main function)
def getProductGrp(broadcastProductPlatform:Broadcast[Map[String,(String, String,String)]]) = {
val getProductGroup = (tier2:String,tier3:String,productNumber:String,item_id:String,organization_Id:String,pnl:String,subpnl:String,operational_depart:String,management_entity:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("0").toDouble.toInt.toString.trim
val Operational_department = Option(operational_depart).getOrElse("").trim.toUpperCase
val ME = Option(management_entity).getOrElse("").trim.toUpperCase
val product_group_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._2
val sub_product_line_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._3

val product_group_ga = 
    if (product_group_plr == null || product_group_plr == "" ) {
        if(tier3 == "M&D" && Operational_department.substring(0,Math.min(3, Operational_department.length)) == "SVC")  "SERVICES"
        else if(tier3 == "PROTECTION & CONTROL" )
        {   if(productNumberO.contains("-") )  "GATEWAYS PARTS" else "PARTS"  } 
        
        else "TBD" }
    else product_group_plr.toString
   
val product_group_pd = 
    if (pnl == "ANASCO") product_group_plr
   else if(pnl =="CLEARWATER" || pnl =="SOWO") sub_product_line_plr
   else if(tier2 == "CAPACITORS" && tier3 == "CAPACITORS") product_group_plr
   else if(tier2 == "CAPACITORS" && tier3 == "NETWORK REGS & TRANS") subpnl
   else if(tier2 == "ACS" ) subpnl
   else if(tier2 == "XD PRIMARY EQUIPMENT") "XD PRIMARY EQUIPMENT"
   else subpnl
  
val product_group_others = product_group_plr

val result = if(ME.contains("MDS")) "INTEGRATION"
             else if(Array("J700", "J70S", "J74I","J74J","J7P","J7PA","J7PS","J74PA","CAPJ").contains(ME)) "AUTOMATION PROJECTS"
             else if(ME == "USPJ") "PMCS"
             else if(Array("SPUK", "J74H", "CAPK", "USPK").contains(ME)) ""
             else if(Array("5750", "J74H", "DG14", "DG24").contains(ME)) ""
             else if  (Array("XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS").contains(Option(tier2).getOrElse("")))  product_group_pd
             else if (tier2 == "GRID AUTOMATION") product_group_ga  
             else product_group_others             
result
}
getProductGroup  
}


//FUnction definition for Product_Line (getProductLine is main function)
def getProductLn(broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getProductLine = (tier2:String,productNumber:String,item_id:String,organization_Id:String,organiz_unit:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("0").toDouble.toInt.toString.trim
val Organ_unit = Option(organiz_unit).getOrElse("").trim.toUpperCase

val product_line_plr = broadcastCatalog.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastCatalog.value.getOrElse(productNumberO,("","","")))._1

val product_line_others = if( ! Array("IO MULTILIN CA","IO MULTILIN PR","IO MULTILIN US","IO CLEARWATER", "IO FT.EDWARD", "IO MT.JULIET","IO LARGO", "IO ROCHESTER").contains(Organ_unit)) "T&D Products"
                       else product_line_plr
                       
val result = if (Array("GRID AUTOMATION","XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS").contains(Option(tier2).getOrElse("")))  product_line_plr            
             else product_line_others             
result
}
getProductLine  
}

//FUnction definition for Product_Segment (getProductSegment is main function)
def getProductSg(broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getProductSegment = (tier2:String,productNumber:String,item_id:String,organization_Id:String,organiz_unit:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("0").toDouble.toInt.toString.trim
val Organ_unit = Option(organiz_unit).getOrElse("").trim.toUpperCase

val product_segment_plr = broadcastCatalog.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastCatalog.value.getOrElse(productNumberO,("","","")))._2

val product_segment_others = if( ! Array("IO MULTILIN CA","IO MULTILIN PR","IO MULTILIN US","IO CLEARWATER", "IO FT.EDWARD", "IO MT.JULIET","IO LARGO", "IO ROCHESTER").contains(Organ_unit)) "T&D Products"
                       else product_segment_plr
                       
val result = if (Array("GRID AUTOMATION","XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS").contains(Option(tier2).getOrElse("")))  product_segment_plr            
             else product_segment_others             
result
}
getProductSegment  
}

//FUnction definition for Business_Segment (getBusinessSegment is main function)
def getBusinessSeg(broadcastProductPlatform:Broadcast[Map[String,(String, String,String)]],broadcastSubBusiness:Broadcast[Map[(String,String),String]]) = {
val getBusinessSegment = (tier2:String,tier3:String,productNumber:String,item_id:String,organization_Id:String,pnl:String,subpnl:String,organiz_unit:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("0").toDouble.toInt.toString.trim
val Organ_unit = Option(organiz_unit).getOrElse("").trim.toUpperCase
val product_platform_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._1
val product_group_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._2
val sub_product_line_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,("","","")))._3

val product_platform_ga = 
    if (product_platform_plr == null || product_platform_plr == "" ) {
        if(tier2 == "GRID AUTOMATION" && tier3 == "M&D") sub_product_line_plr
        else if(tier3 == "PROTECTION & CONTROL" )
        {   if(productNumberO.contains("-") )  "GATEWAYS/RTUS/SAS" else "Accessories"  } 
      else "TBD" }
    else product_platform_plr.toString
   
val business_segment_ga = if ( broadcastSubBusiness.value.getOrElse((product_platform_ga,tier3),tier3) == null ) tier3 else broadcastSubBusiness.value.getOrElse((product_platform_ga,tier3),tier3)
  
val result = if (tier2 == "GRID AUTOMATION") business_segment_ga  
             else tier3 
                         
result
}
getBusinessSegment  
}

def getProductCl(broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getProductClass = (tier2:String,productNumber:String,item_id:String,organization_Id:String,organiz_unit:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("0").toDouble.toInt.toString.trim
val Organ_unit = Option(organiz_unit).getOrElse("").trim.toUpperCase

val product_class_plr = broadcastCatalog.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastCatalog.value.getOrElse(productNumberO,("","","")))._1

val V_PRODUCTCLASS = if (product_class_plr == null || product_class_plr == "") "OTHERS" else product_class_plr

val result = if( ! Array("IO MULTILIN CA","IO MULTILIN PR","IO MULTILIN US","IO CLEARWATER", "IO FT.EDWARD", "IO MT.JULIET","IO LARGO", "IO ROCHESTER").contains(Organ_unit)) "T&D Products"
                       else V_PRODUCTCLASS
                                  
result
}
getProductClass  
}

def getManagementEnt(broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getManagementEntity = (organiz_unit:String,MANAGEMENT_ENTITY:String,MANAGEMENT_ENTITY_PLR:String,LKP_ME_REF:String) => { 
val Organ_unit = Option(organiz_unit).getOrElse("").trim.toUpperCase

val arr1 = Array("IO MULTILIN CA","IO MULTILIN PR","IO MULTILIN US","IO CLEARWATER", "IO FT.EDWARD", "IO MT.JULIET","IO LARGO", "IO ROCHESTER")

val expr1 = if (LKP_ME_REF == null)  MANAGEMENT_ENTITY  else  LKP_ME_REF
val V_MANAGEMENT_ENTITY_PLR = if (MANAGEMENT_ENTITY_PLR == null || MANAGEMENT_ENTITY_PLR == "" || MANAGEMENT_ENTITY_PLR == "0")  expr1  else  MANAGEMENT_ENTITY_PLR
val res = if (arr1.contains(organiz_unit))  V_MANAGEMENT_ENTITY_PLR  else  MANAGEMENT_ENTITY
res
}
getManagementEntity  
}


def getProductGpL(broadcastProductGroup:Broadcast[Map[String,(String, String,String)]]) = {
val getProductGroupL = (productNumber:String,organization_Id:String,product_class:String,product_group:String,DATA_SOURCE:String,management_entity:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("").trim
val prod_class = Option(product_class).getOrElse("").trim.toUpperCase
val prod_group = Option(product_group).getOrElse("").trim.toUpperCase
val ME = Option(management_entity).getOrElse("").trim.toUpperCase
val product_group_plr = broadcastProductGroup.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductGroup.value.getOrElse(productNumberO,
broadcastProductGroup.value.getOrElse(productNumberO.replaceAll("-",""),
broadcastProductGroup.value.getOrElse(prod_class,
broadcastProductGroup.value.getOrElse(productNumberO.substring(0,Math.min(4, productNumberO.length)) ,
broadcastProductGroup.value.getOrElse(productNumberO.substring(0,Math.min(3, productNumberO.length)), 
("","","")))))))._2

val product_group_prod = if (product_group_plr == null || product_group_plr =="" ) {
                                 if (DATA_SOURCE == "BRAZIL") "PACKAGED SOLUTIONS" 
                                 else "TBD"
                               }
                            else product_group_plr

val productNumber1 = productNumberO.substring(0,Math.min(1, productNumberO.length) )   
val    productNumber2 =    productNumberO.substring(0,Math.min(2, productNumberO.length))        
val product_group_project =  
         if(DATA_SOURCE == "SPAIN"){
             if(productNumber1 == "1" || productNumber2 == "60" ) "CONSULTING BILBAO"
             else if (Array("2", "3", "4", "5", "9").contains(productNumber1) || Array("63", "66", "70").contains(productNumber2)) "PACKAGED SOLUTIONS"
             else product_group_plr
         }             
         else{
         if(ME.contains("MDS")) "INTEGRATION"
         else if(Array("J700", "J70S", "J74I","J74J","J7P","J7PA","J7PS","J74PA","CAPJ").contains(ME)) "AUTOMATION PROJECTS"
         else if(ME == "USPJ") "PMCS"
         else if(Array("SPUK", "J74H", "CAPK", "USPK").contains(ME)) ""
         else if(Array("5750", "J74H", "DG14", "DG24").contains(ME)) ""
         else product_group_prod
         }
          
product_group_project
}
getProductGroupL 
}

def getProductPlatL(broadcastProductPlatform:Broadcast[Map[String,(String, String,String)]]) = {
val getProductPlatformL = (productNumber:String,organization_Id:String,product_class:String,product_group:String,DATA_SOURCE:String,management_entity:String,product_platform:String,Project_number:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("").trim
val prod_class = Option(product_class).getOrElse("").trim.toUpperCase
val prod_group = Option(product_group).getOrElse("").trim.toUpperCase
val ME = Option(management_entity).getOrElse("").trim.toUpperCase
val projectnumber = Option(Project_number).getOrElse("").trim.toUpperCase
val productplatform =Option(product_platform).getOrElse("").trim.toUpperCase

val product_platform_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,
broadcastProductPlatform.value.getOrElse(productNumberO.replaceAll("-",""),
broadcastProductPlatform.value.getOrElse(prod_class,
broadcastProductPlatform.value.getOrElse(productNumberO.substring(0,Math.min(4, productNumberO.length)) ,
broadcastProductPlatform.value.getOrElse(productNumberO.substring(0,Math.min(3, productNumberO.length)), 
(productplatform,"","")))))))._1

val product_platform_prod = if (product_platform_plr == null || product_platform_plr =="" ) {
                                 if (DATA_SOURCE == "BRAZIL") "PACKAGED SOLUTIONS" 
                                 else "TBD"
                               }
                            else product_platform_plr

val projectnumber1 = projectnumber.substring(0,Math.min(1, projectnumber.length) )   
val    projectnumber2 =    projectnumber.substring(0,Math.min(2, projectnumber.length))        

val product_platform_project =  
         if(DATA_SOURCE == "SPAIN"){
             if(projectnumber1 == "1" || projectnumber2 == "60" ) "CONSULTING"
             else if (Array("2", "3", "4", "5", "9").contains(projectnumber1) || Array("63", "66", "70").contains(projectnumber2)) "PACKAGED SOLUTIONS"
             else product_platform_plr
         }             
         else{
         if(ME.contains("MDS")) "PROJECT"
         else if(Array("J700", "J70S", "J74I","J74J","J7P","J7PA","J7PS","J74PA","CAPJ").contains(ME)) "AUTOMATION PROJECTS"
         else if(ME == "USPJ") "IEMS"
         else if(Array("SPUK", "J74H", "CAPK", "USPK").contains(ME)) "PACKAGED SOLUTIONS"
         else if(Array("5750", "J74H", "DG14", "DG24").contains(ME)) "T&D PRODUCTS"
         else product_platform_prod
         }
          
product_platform_project
}
getProductPlatformL 
}

def getBusinessSegL(broadcastSubBusiness:Broadcast[Map[(String,String),String]],broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getBusinessSegmentL = (productNumber:String,organization_Id:String,product_class:String,product_group:String,tier2:String,tier3:String,productPlatform:String) => { 

val product_Platform  = Option(productPlatform).getOrElse("")

val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).getOrElse("").trim
val prod_class = Option(product_class).getOrElse("").trim.toUpperCase
val prod_group = Option(product_group).getOrElse("").trim.toUpperCase

val product_line_plr = broadcastCatalog.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastCatalog.value.getOrElse(productNumberO,
broadcastCatalog.value.getOrElse(productNumberO.replaceAll("-",""),
broadcastCatalog.value.getOrElse(prod_class,
broadcastCatalog.value.getOrElse(productNumberO.substring(0,Math.min(4, productNumberO.length)) ,
broadcastCatalog.value.getOrElse(productNumberO.substring(0,Math.min(3, productNumberO.length)), 
("","","")))))))._1
  
val business_segment_ga = if (tier3 == "M&D")
                             product_line_plr
                    else{ 
                              if ( broadcastSubBusiness.value.getOrElse((product_Platform,tier3),tier3) == null ) tier3 
                       else broadcastSubBusiness.value.getOrElse((product_Platform,tier3),tier3)
                       }
  
val result = if (tier2 == "GRID AUTOMATION") business_segment_ga  
             else ""
                         
result
}
getBusinessSegmentL  
}


def getOrderPL(broadcastSubBusiness:Broadcast[Map[(String,String),String]]) = {
val getOrderPulseL = (tier2:String,tier3:String,data_source:String,order_type_pulse:String,productPlatform:String) => { 
val dataSource  = Option(data_source).getOrElse("").trim.toUpperCase
val product_Platform  =  Option(productPlatform).getOrElse("").toUpperCase 
val ordertypepulse  = if(dataSource == "SPAIN") Option(order_type_pulse).getOrElse("").toUpperCase else "PRODUCT_ORDERS"

  
val order_pulse_ga = if (tier3 == "PROTECTION & CONTROL" ||  tier3 == "SAS")
                            {
                            val sub_business = broadcastSubBusiness.value.getOrElse((product_Platform,tier3),"")
                            if ( sub_business == null ||  sub_business == "" ) ordertypepulse 
                            else sub_business
                            }
                    else  ordertypepulse
                                               
val result = if (tier2 == "GRID AUTOMATION") order_pulse_ga  
             else ordertypepulse                         
result
}
getOrderPulseL  
}




def getProductLnL(broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getProductLineL = (tier2:String,productNumber:String,organization_Id:String,product_class:String,product_group:String,product_line:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).toString.trim
val productline = Option(product_line).getOrElse("").trim.toUpperCase
val prod_class = Option(product_class).getOrElse("").trim.toUpperCase
val prod_group = Option(product_group).getOrElse("").trim.toUpperCase

val product_line_plr = broadcastCatalog.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastCatalog.value.getOrElse(productNumberO,
broadcastCatalog.value.getOrElse(productNumberO.replaceAll("-",""),
broadcastCatalog.value.getOrElse(prod_class,
broadcastCatalog.value.getOrElse(productNumberO.substring(0,Math.min(4, productNumberO.length)) ,
broadcastCatalog.value.getOrElse(productNumberO.substring(0,Math.min(3, productNumberO.length)), 
("","","")))))))._1
                       
val result = if (Array("GRID AUTOMATION","XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS").contains(Option(tier2).getOrElse("")))  product_line_plr            
             else productline             
result
}
getProductLineL  
}



def getSubProductLnL(broadcastProductPlatform:Broadcast[Map[String,(String, String,String)]]) = {
val getSubProductLineL = (tier2:String,productNumber:String,organization_Id:String,product_class:String,product_group:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).toString.trim
val prod_class = Option(product_class).getOrElse("").trim.toUpperCase
val prod_group = Option(product_group).getOrElse("").trim.toUpperCase

val sub_product_line_plr = broadcastProductPlatform.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastProductPlatform.value.getOrElse(productNumberO,
broadcastProductPlatform.value.getOrElse(productNumberO.replaceAll("-",""),
broadcastProductPlatform.value.getOrElse(prod_class,
broadcastProductPlatform.value.getOrElse(productNumberO.substring(0,Math.min(4, productNumberO.length)) ,
broadcastProductPlatform.value.getOrElse(productNumberO.substring(0,Math.min(3, productNumberO.length)), 
("","","")))))))._3
                       
val result =   sub_product_line_plr           
result
}
getSubProductLineL  
}


def getProductSgmtL(broadcastCatalog:Broadcast[Map[String,(String, String,String)]]) = {
val getProductSegmentL = (tier2:String,productNumber:String,organization_Id:String,product_class:String,product_group:String) => { 
val productNumberO = Option(productNumber).getOrElse("").trim
val organizationId =  Option(organization_Id).toString.trim
val prod_class = Option(product_class).getOrElse("").trim.toUpperCase
val prod_group = Option(product_group).getOrElse("").trim.toUpperCase

val product_segment_plr = broadcastCatalog.value.getOrElse(productNumberO.concat(",").concat(organizationId),
broadcastCatalog.value.getOrElse(productNumberO,
broadcastCatalog.value.getOrElse(productNumberO.replaceAll("-",""),
broadcastCatalog.value.getOrElse(prod_class,
broadcastCatalog.value.getOrElse(productNumberO.substring(0,Math.min(4, productNumberO.length)) ,
broadcastCatalog.value.getOrElse(productNumberO.substring(0,Math.min(3, productNumberO.length)), 
("","","")))))))._2
                       
val result =   product_segment_plr           
result
}
getProductSegmentL  
} 


}
