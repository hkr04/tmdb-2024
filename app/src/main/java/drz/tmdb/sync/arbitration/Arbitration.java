package drz.tmdb.sync.arbitration;

public class Arbitration {
    private static int copyNum;//副本数量

    private static int minWriteNum;//达成写成功所需的最小写请求数

    private static int minReadNum;//达成读成功所需额最小读请求数

    private int count = 0;//计数器



    public static void setCopyNum(int copyNum) {
        Arbitration.copyNum = copyNum;
    }

    public static void setMinWriteNum(int minWriteNum) {
        Arbitration.minWriteNum = minWriteNum;
    }

    public static void setMinReadNum(int minReadNum) {
        Arbitration.minReadNum = minReadNum;
    }

    public static void initialConfig(int copyNum, int minWriteNum, int minReadNum){
        if(copyNum <= minWriteNum + minReadNum){
            System.out.println("仲裁配置参数不符要求，请重新配置！");
            return;
        }

        Arbitration.copyNum = copyNum;
        Arbitration.minWriteNum = minWriteNum;
        Arbitration.minReadNum = minReadNum;
    }
}
