import java.io.IOException;

/**
 * Created by fengxinlin on 3/4/17.
 */
public class Driver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        CellMultiplication cellMultiplication = new CellMultiplication();
        CellSum cellSum = new CellSum();
        //args0: dir of transition
        //args1: dir of PageRank
        //args2: dir of unitMultiplication result
        //args3: times of convergence
        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String unitState = args[2];
        int count = Integer.parseInt(args[3]);
        for (int i = 0;  i < count;  i++) {
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i};
            cellMultiplication.main(args1);
            String[] args2 = {unitState + i, prMatrix+(i+1)};
            cellSum.main(args2);
        }

    }
}
