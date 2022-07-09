import java.io.PrintStream;
import java.util.Calendar;
import java.util.Random;

public class MakeData {
    private static String[] productName={"一汽集团","一汽丰田销售","一汽-大众","吉林汽车","轿车公司","解放公司",
            "天津夏利","客车公司","通用公司","红旗事业部","一汽海马"};
    private static String[] comCode={"0123456789","6051206979","2449764139","1030718999","7430287259",
            "7430120299","71530004X9","1244832780","7765904829","7025003680","42851028X0"};
//    private static String[] comName={"东软汽车销售总部","东软汽车销售一部","东软汽车销售二部","东软汽车销售三部",
//            "东软汽车销售四部","东软汽车销售五部","东软汽车销售六部","东软汽车销售七部",
//            "东软汽车销售八部","东软汽车销售九部","东软汽车销售十部"};
//    private static String[] saleColumn={"职工薪酬","运输费","促销费","产品质量保证费","业务宣传费","广告费"};
    public static void main(String args[])throws Exception{
        Random rand=new Random();
        Calendar cd=Calendar.getInstance();
        //cd里只有月份是特别的，从0开始到11
        //产品单位日表
        cd.set(2016,0,1);
        while(cd.get(Calendar.YEAR)!=2022){
            int y=cd.get(Calendar.YEAR),m=cd.get(Calendar.MONTH)+1,d=cd.get(Calendar.DATE);
            String time=Integer.toString(y*10000+m*100+d);
            String path="src/main/java/car_origin_data/product_day/product_"+time+".csv";
            System.setOut(new PrintStream(path));
            for(String name:productName){
                int n1=450+rand.nextInt(451);
                int n2=300+rand.nextInt(301);
                int n3=450+rand.nextInt(451);
                System.out.println(name+","+y+","+m+","+d+","+n1+","+n2+","+n3);
            }
            cd.add(Calendar.DATE,1);
        }
        //销售单位日表
        cd.set(2016,0,1);
        while(cd.get(Calendar.YEAR)!=2022){
            int y=cd.get(Calendar.YEAR),m=cd.get(Calendar.MONTH)+1,d=cd.get(Calendar.DATE);
            String time=Integer.toString(y*10000+m*100+d);
            String path="src/main/java/car_origin_data/sale_day/sale_"+time+".csv";
            System.setOut(new PrintStream(path));
            for(String code:comCode){
                double n1=6000000+rand.nextInt(21000001)+rand.nextDouble();
                double n2=500000000+rand.nextInt(100000001)+rand.nextDouble();
                double n3=75000000+rand.nextInt(60000001)+rand.nextDouble();
                double n4=300000000+rand.nextInt(300000001)+rand.nextDouble();
                System.out.printf("%s,%d,%d,%d,%.2f,%.2f,%.2f,%.2f\n",code,y,m,d,n1,n2,n3,n4);
            }
            cd.add(Calendar.DATE,1);
        }
        //销售单位月表
        cd.set(2016,0,1);
        while(cd.get(Calendar.YEAR)!=2022){
            int y=cd.get(Calendar.YEAR),m=cd.get(Calendar.MONTH)+1;
            String time=Integer.toString(y*100+m);
            String path="src/main/java/car_origin_data/sale_month/sale_"+time+".csv";
            System.setOut(new PrintStream(path));
            for(String code:comCode){
                double n7=27000000000L+Math.abs(rand.nextLong())%4000000001L+rand.nextDouble();
                double n8=36000000000L+rand.nextInt(900000001)+rand.nextDouble();
                double n9=Math.abs(rand.nextLong())%3200000001L+rand.nextDouble();
                System.out.printf("%s,%d,%d",code,y,m);
                for(int i=1;i<=6;i++){
                    double n1_6=9000000+rand.nextInt(46000001)+rand.nextDouble();
                    System.out.printf(",%.2f",n1_6);
                }
                System.out.printf(",%.2f,%.2f,%.2f",n7,n8,n9);
                for(int i=10;i<=25;i++){
                    double n10_25=900000000+rand.nextInt(900000001)+rand.nextDouble();
                    if(i==22){
                        if(rand.nextInt(2)==1) System.out.printf(",%.2f",-n10_25);
                        else System.out.printf(",%.2f",n10_25);
                    }
                    else System.out.printf(",%.2f",n10_25);
                }
                System.out.println();
            }
            cd.add(Calendar.MONTH,1);
        }
    }
}