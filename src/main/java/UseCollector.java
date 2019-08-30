
public class UseCollector {
    public static void useCollector(Collector collector, int num, String content) {
        System.out.println(collector.getName(Long.valueOf(num), content));
    }
    public static void main(String[] args) {
//        useCollector(new Collector() {
//            @Override
//            public String getName(Long num, String content) {
//                return String.valueOf(num) + " " + content;
//            }
//        });

        useCollector((n, content) -> String.valueOf(n) + " ...." + content,
                30,
                "hello em");
    }
}
