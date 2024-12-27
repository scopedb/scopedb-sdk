package io.scopedb.sdk.client;

import de.vandermeer.asciitable.AsciiTable;
import java.util.Arrays;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * @see <a href="http://www.vandermeer.de/projects/skb/java/asciitable/examples.html">asciitable's docs</a>
 */
@UtilityClass
public class PrettyPrint {
    public static String renderArrowRecordBatch(List<VectorSchemaRoot> batches) {
        final AsciiTable table = new AsciiTable();
        return table.render();
    }

    public static void main(String[] args) {
        String[][] table = {{"中国", "Bloggs", "18"},
                {"Steve", "Jobs", "20"},
                {"George", "Cloggs", "测试"}};

        for(int i=0; i<3; i++){
            for(int j=0; j<3; j++){
                System.out.print(String.format("%20s", table[i][j]));
            }
            System.out.println("");
        }

    }
}
