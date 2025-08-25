public class Ex01 {
    static void may() throws Exception { throw new Exception("e"); }
    public static void main(String[] args) { try { may(); } catch (Exception e) { System.exit(1); } }
}
