import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IDEA by User1071324110@qq.com
 *
 * @author 10713
 * @date 2018/7/4 23:20
 */
@Data
public class MyStorm {
    private Random random = new Random();

    private BlockingQueue sentenceQueue = new ArrayBlockingQueue(50000);
    private BlockingQueue wordQueue = new ArrayBlockingQueue(50000);
    //用来保存最后计算的结果 key = 单词， value = 单词个数
    Map<String, Integer> counters = new HashMap<String, Integer>();

    //用来发句子
    public void nextTuple() {
        String[] sentences = new String[]{"the   cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs", "i am at two with nature"};
        String sentence = sentences[random.nextInt(sentences.length)];
        try {
            sentenceQueue.put(sentence);
            System.out.println("send sentence : " + sentence);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void spilt(String sentence) {
        System.out.println("resv sentence " + sentence);
        String[] words = sentence.split(" ");
        for (String word : words) {
            //清除多余空格
            word = word.trim();
            // 三个空格的时候“空格空格空格” 会拆分成两个空的字符串“”，“”
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                //collect。emit（）
                wordQueue.add(word);
                System.out.println("split word :" + word);
            }
        }
    }

    //用来计算单词
    public void wordCounter(String word) {
        if (!counters.containsKey(word)) {
            counters.put(word, 1);
        } else {
            Integer c = counters.get(word) + 1;
            counters.put(word, c);
        }
        System.out.println("print map :" + counters);
    }

    public static void main(String[] args) {
        //线程池
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        MyStorm myStorm = new MyStorm();
        //发射句子到sentenceQueue
        executorService.submit(new MySpout(myStorm));
        //接受一个句子，并将句子切割
        executorService.submit(new MyBoltSplit(myStorm));
        //接受一个单词，并进行计算
        executorService.submit(new MyWordCount(myStorm));
    }


}

class MySpout extends Thread {
    private MyStorm myStorm;

    public MySpout(MyStorm myStorm) {
        this.myStorm = myStorm;
    }

    @Override
    public void run() {
        //storm框架在循环调用spout的nextTuple方法
        while (true) {
            myStorm.nextTuple();
            try {
                sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class MyBoltSplit extends Thread {

    private MyStorm myStorm;

    public MyBoltSplit(MyStorm myStorm) {
        this.myStorm = myStorm;
    }

    @Override
    public void run() {
        while (true) {
            try {
                String sentence = (String) myStorm.getSentenceQueue().take();
                myStorm.spilt(sentence);
                sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class MyWordCount extends Thread {
    private MyStorm myStorm;

    public MyWordCount(MyStorm myStorm) {
        this.myStorm = myStorm;
    }

    @Override
    public void run() {
        while (true) {
            try {
                String word = (String) myStorm.getWordQueue().take();
                myStorm.wordCounter(word);
                sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
