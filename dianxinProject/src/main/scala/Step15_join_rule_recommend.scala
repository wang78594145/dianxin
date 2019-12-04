package dianxinProject;

object Step15_join_rule_recommend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FPGrowth").setMaster("local")
    val sc = new SparkContext(conf)
    /* 读取有预购需求的用户数据
       用户id|产品类型：访问次数
     */
    val recommend_user = sc.textFile("file:/E:\\dx_proj\\output\\Step10_order_buy_user")
      .map(x=>(x.split("\\|")(0), ""))
    /* 读取行为匹配输出数据
       行为id|用户号码|是否产品|url|预购类型
     */
    val action_match = sc.textFile("file:/E:\\dx_proj\\output\\Step2_action_match")
      .map(x=>(x.split("\\|")(1), x.split("\\|")(0)))
    // 做表关联：行为ID，用户ID
    val user_action = recommend_user.join(action_match).distinct().map(x=>(x._2._2, x._1))
    /* 读取配置数据
       000001000000000000,互联网|产品|保险|平台|推广|网|零零
     */
    val basic_word_split = sc.textFile("file:/C:\\Users\\xc\\Desktop\\项目资料\\数据\\配置数据\\basic_word_split.txt")
      .map(x=>(x.split(",")(0), x.split(",")(1)))
    // 做表关联：用户ID，word
    val user_word = user_action.join(basic_word_split).distinct().map(x=>(x._2._1, x._2._2)).reduceByKey(_+"|"+_)
      // 从word中提取出现次数大于均值的高频词
      .map(x=>{
        val sb = new StringBuffer() //存高频词
        val splits = x._2.split("\\|")
        val words = mutable.HashMap[String, Int]() //存word，并统计每个word的数量
        for (split <- splits){
          if (words.contains(split)){
            words.put(split, words.get(split).get+1)
          }else{
            words.put(split, 1)
          }
        }
        // 判断大于均值的高频词，并且存到sb中
        val avg = splits.length/words.size
//        println("avg:"+avg+";sum:"+splits.length+";num:"+words.size)
        for (word <- words){
          if(word._2 >= avg){
            sb.append(word._1+",")
          }
        }
        (x._1, sb)
      });
    // 读取关联规则输出数据： 关联词，推荐产品分类
    val join_rule = sc.textFile("file:/E:\\dx_proj\\output\\Step14_join_rule")
      .map(x=>(x.split("\\|")(1), x.split("\\|")(0))).collect().toMap
    //
    val userID_proType = user_word.map(x=>{
      val userWord = x._2.toString.substring(0, x._2.length()-1)
      var size = 0
      for (rule <- join_rule){
        if (userWord.contains(rule._2) && rule._2.size>size){
          size = rule._2.size
        }
      }
      size
    }).foreach(println);




  }


}
