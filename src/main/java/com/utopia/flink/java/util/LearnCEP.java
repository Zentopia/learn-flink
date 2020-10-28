package com.utopia.flink.java.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import java.util.List;

public class LearnCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         *  接收source并将数据转换成一个tuple
         */
        DataStream<Tuple3<String, String, String>> myDataStream = env.addSource(
                new OrderSource()).map((MapFunction<String, Tuple3<String, String, String>>) value -> {
                    JSONObject json = JSON.parseObject(value);
                    return new Tuple3<>(json.getString("userid"),json.getString("orderid"),json.getString("behave"));
                }).returns(TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {}));

        /**
         * 定义一个规则
         * 接受到behave是order以后，下一个动作必须是pay才算符合这个需求
         */
        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> myPattern = Pattern.<Tuple3<String, String, String>>begin("start").where(new IterativeCondition<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> context) throws Exception {
                System.out.println("value:" + value);
                return value.f2.equals("order");
            }
        }).next("next").where(new IterativeCondition<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> context) throws Exception {
                return value.f2.equals("pay");
            }
        }).within(Time.seconds(3));


        PatternStream<Tuple3<String, String, String>> pattern = CEP.pattern(myDataStream.keyBy(0), myPattern);

        //记录超时的订单
        OutputTag<String> outputTag = new OutputTag<String>("myOutput"){};

        SingleOutputStreamOperator<String> resultStream = pattern.select(outputTag,
                //超时的
                (PatternTimeoutFunction<Tuple3<String, String, String>, String>) (pattern1, timeoutTimestamp) -> {
                    System.out.println("pattern:"+ pattern1);
                    List<Tuple3<String, String, String>> startList = pattern1.get("start");
                    Tuple3<String, String, String> tuple3 = startList.get(0);
                    return tuple3.toString() + "迟到的";
                },

                (PatternSelectFunction<Tuple3<String, String, String>, String>) pattern12 -> {
                    //匹配上第一个条件的
                    List<Tuple3<String, String, String>> startList = pattern12.get("start");
                    //匹配上第二个条件的
                    List<Tuple3<String, String, String>> endList = pattern12.get("next");

                    Tuple3<String, String, String> tuple3 = endList.get(0);
                    return tuple3.toString();
                }
        );

        //输出匹配上规则的数据
        resultStream.print();

        //输出超时数据的流
        DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print();

        env.execute("Test CEP");
    }


}
