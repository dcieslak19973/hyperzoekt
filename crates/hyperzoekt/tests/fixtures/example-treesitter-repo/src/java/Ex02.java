import java.io.*;
class Ex02 {
    static void run() {
        try (ByteArrayInputStream b = new ByteArrayInputStream(new byte[0])) {
        } catch (IOException e) { }
    }
}
