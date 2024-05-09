package libtagpropagation;
import utils.KafkaConfig;

import java.math.BigInteger;
import java.util.ArrayList;

public class ParseTester {
    public static void main(String[] args) throws Exception{

//        1648196412041127565    1648405875429715088    ubantu benign
//        durationTime("1648196412041127565", "1648405875429715088");
//        1648196053351249642 (后)  1648471346229724669 （前）     ubantu anomly
//        durationTime("1648196053351249642", "1648471346229724669");
//        System.out.println(0.5 * 3600000000000L);

        KafkaConfig kafkaConfig = new KafkaConfig();
        ArrayList<String> kafkaTopicList = kafkaConfig.getTopicList(1,2);
        for (String topic : kafkaTopicList) {
            System.out.println(topic);
        }
    }

    public static void durationTime(String startTime, String endTime){
//         将字符串转换为 BigInteger 对象
        BigInteger num1 = new BigInteger(startTime);
        BigInteger num2 = new BigInteger(endTime);
        String strNum3 = "3600000000000";
        BigInteger num3 = new BigInteger(strNum3);
        String strNum4 = "60000000000";
        BigInteger num4 = new BigInteger(strNum4);

        // 执行大数相减
//        BigInteger result = num1.divide(num2);
        BigInteger result = num2.subtract(num1);
        BigInteger[] results = result.divideAndRemainder(num3);

        // 输出结果
        System.out.println("Result of subtraction: " + results[0] + " 余： " + results[1].divide(num4));
    }
}
