package dianxinProject

object Step12_ALS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local")
    val sc = new SparkContext(conf)
    /*
        读取源数据
        产品ID | 用户ID | 访问次数 | 产品类型(car=1/house=2)
        car000001|513050157717|1|car
     */
    val data_source = sc.textFile("file:/F:\\dx_proj\\output\\Step6_user_product_count\\part-r-00000")
    /*
        (用户ID|用户ID的哈希值, 产品ID|产品ID的哈希值, 访问次数|产品类型)
     */
    val basicId = data_source.map(x=>x.split("\\|")).map(x=>(x(1)+"|"+x(1).hashCode, x(0)+"|"+x(0).hashCode, x(2)+"|"+x(3)))
    // map集合：(用户ID的哈希值，用户ID)
    val map_user = basicId.map(x => (x._1.split("\\|")(1), x._1.split("\\|")(0))).collect().toMap
    // map集合：(产品ID的哈希值，产品ID)
    val map_product = basicId.map(x => (x._2.split("\\|")(1), x._2.split("\\|")(0))).collect().toMap
    /*
        根据现有数据创建训练集
        new Rating(用户ID的哈希值(Int), 产品ID的哈希值(Int), 访问次数(即相当于评分Float))
     */
    val ratings = basicId.map(x=>{new Rating(x._1.split("\\|")(1).toInt, x._2.split("\\|")(1).toInt, x._3.split("\\|")(0).toFloat)})
    // 设置隐性因子为5
    val rank = 5
    // 设置迭代次数为10，默认为10
    val numIterations = 10
    // 建立模型，0.01-->正则化系数
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    /*
        ----------------------------------------------------------------------------
        ----------------------------------------------------------------------------
     */

    // (用户ID的哈希值, 预购类型), 去重
    val rdd_user = basicId.map(x=>(x._1.split("\\|")(1), x._3.split("\\|")(1))).distinct()
    // map集合：(用户ID的哈希值，预购类型(car/house))
    val map_type = rdd_user.collect().toMap
    // (产品ID的哈希值), 去重
    val rdd_product = basicId.map(x=>x._2.split("\\|")(1)).distinct()
    // 对"用户ID的哈希值"和"产品ID的哈希值"做笛卡尔积
    val user_product = rdd_user.map(x=>x._1).cartesian(rdd_product)
    /*
        根据model.predict方法得到每一个用户对每一个产品的评分，返回值类型：RDD[Rating]
        参数为：(用户ID的哈希值(Int), 产品ID的哈希值(Int))
     */
    val useProScore = model.predict(user_product.map(x=>(x._1.toInt, x._2.toInt)))
        /*
            根据哈希值得到对应的用户ID，产品ID
            (用户ID|产品类型, 产品ID|评分)
         */
          .map(x=>(map_user(x.user.toString)+"|"+map_type(x.user.toString), map_product(x.product.toString)+"|"+x.rating))
        // 过滤掉评分小于零的数据，因为用户不关心该产品
        .filter(x=>x._2.split("\\|")(1).toDouble>0)
        // 过滤掉不属于该用户产品类型的数据，因为用户不需要该产品
        .filter(x=>x._2.split("\\|")(0).contains(x._1.split("\\|")(1)))
        // 对符合的数据进行整合
        .reduceByKey(_+","+_)
        // 对每一个用户，按评分对产品进行排序并取出前5名，然后将产品ID输出
        .map(x=>(x._1, x._2.split(",").sortBy(x=>x.split("\\|")(1).toDouble).reverse.take(5).map(x=>x.split("\\|")(0))))
        // 按格式输出结果：用户ID|预购类型|推荐产品ID列表
        .map(x=>x._1.split("\\|")(0)+"|"+mydef(x._1.split("\\|")(1))+"|"+myPrint(x._2))
        // 将结果保存
        .saveAsTextFile("file:/F:\\dx_proj\\output\\Step12_ALS")

  }

  def myPrint(array: Array[String]): String = {
    var s = array(0)
    for(i <- 1 to (if(array.length>5) 4 else array.length-1)){
      s = s+","+array(i)
    }
    return s
  }
  def mydef(string: String): Int = {
    if(string.equals("car")){
      return 1
    }else if(string.equals("house")){
      return 2
    }
    return -1
  }

}
