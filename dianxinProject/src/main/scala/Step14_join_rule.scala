package dianxinProject;


object Step14_join_rule {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FPGrowth").setMaster("local")
    val sc = new SparkContext(conf)

    // 000001000000000000,互联网|产品|保险|平台|推广|网|零零
    val basic_word_split = sc.textFile("file:/C:\\Users\\xc\\Desktop\\项目资料\\数据\\配置数据\\basic_word_split.txt")
      .map(x=>(x.split(",")(0), x.split(",")(1)))
    // 1|car000001|1
    val car_type = sc.textFile("file:/C:\\Users\\xc\\Desktop\\项目资料\\数据\\配置数据\\car_type.txt")
      .map(x=>(x.split("\\|")(1), x.split("\\|")(0)))
    // 行为id|用户id|是否产品|url|预购类型
    val step2 = sc.textFile("file:/E:\\dx_proj\\output\\Step2_action_match\\part-r-00000")
      .map(x=>(x.split("\\|")(0), x.split("\\|")(1)))
    // 029756000001000010|car|car000001|DS-DS 6-无限制|DS|16.39-30.19万|DS 6|欧系其他|无限制
    val proID_itemID =sc.textFile("file:/C:\\Users\\xc\\Desktop\\项目资料\\数据\\配置数据\\t_dx_product_msg_addr.txt")
      .map(x=>(x.split("\\|")(0), x.split("\\|")(2)))

    // (523047653102,house000144)
    val userID_proID = step2.leftOuterJoin(proID_itemID).map(x=>(x._2._1, x._2._2.getOrElse())).filter(x=>(x._2!=()))
    // (693045784733,你|就|百度一下|知道)
    val userID_item = step2.leftOuterJoin(basic_word_split).map(x=>(x._2._1, x._2._2.getOrElse())).filter(x=>(x._2!=()))
    // (car000466,之家|我|汽车|的|网站)
    val proID_item = userID_proID.join(userID_item).map(x=>(x._2._1.toString, x._2._2))
    // (互动|搜狗|旗下|最大|社区|问答|问问,dxpro10)
    val item_type = proID_item.leftOuterJoin(car_type).map(x=>(x._2._1, x._2._2.getOrElse()))
      .filter(x=>(x._2!=())).map(x=>(x._1+ "|dxpro"+ x._2)).map(_.split("\\|"))

    val minSupport = 0.01
    val model = new FPGrowth().setMinSupport(minSupport).run(item_type)

    val minConfidence = 0.15
    model.generateAssociationRules(minConfidence).filter(x=>x.consequent.apply(0).contains("dxpro"))
      .map(x=>(x.antecedent.mkString("",", ",""), x.consequent.apply(0)+"|"+x.confidence)).reduceByKey(_+","+_)
      .map(x=>(x._1, x._2.split(",").sortBy(x=>x.split("\\|")(1).toFloat).reverse.take(1)))
      .map(x=>(x._1+"|"+x._2(0).split("\\|")(0))).foreach(println)

//    val myRDD = item_type.distinct().reduceByKey(_+","+_)

//    // 产品id|用户id|访问次数|产品类型
//    val step6_RDD = sc.textFile("file:/E:\\dx_proj\\output\\Step6_user_product_count\\part-r-00000")
//      .map(x=>(x.split("\\|")(1), x.split("\\|")(0)))

//    // (513043533721,58|com|免费|分类信息|同城|哈尔滨|本地|高效)
//    val userID_word = step2_RDD.leftOuterJoin(basic_word_split).map(x=>(x._2._1, x._2._2.getOrElse())).filter(x=>(x._2!=()))
//    // (house000276,免费|分析|友|好用|平台|广告|推广|效果|的|监测|盟|移动)
//    val proID_word = step6_RDD.join(userID_word).map(x=>(x._2._1, x._2._2))
//    // (中国|代码|分享|图文|工具|强大|按钮|最|的|社会化|网页,dxpro6)
//    // 产业,创造,平台,广联达,建筑,数字,有限公司,服务商,生活,用,科技,美好,股份|dxpro8
//    val word_type = proID_word.leftOuterJoin(car_type).map(x=>(x._2._1, "dxpro"+x._2._2.getOrElse()))
//      .map(x=>(x._1.toString.replace("|", ",")+"|"+x._2))


  }

}
