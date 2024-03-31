package com.arjunsk.sort_merge.inmemory;
import java.io.*;
import java.util.*;

public class SortMergeJoin{
    public static void main(String[] args) throws Exception {
        List<String[]> table1 = readCsv("/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/left_input.csv");
        List<String[]> table2 = readCsv("/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/right_input.csv");

        // Assuming the join key is on the first column for both tables
        Comparator<String[]> comparator = Comparator.comparing(arr -> arr[0]);
        Collections.sort(table1, comparator);
        Collections.sort(table2, comparator);

        List<String[]> joinedTable = sortMergeJoin(table1, table2);

        // Print joined table for demonstration
        for (String[] row : joinedTable) {
            System.out.println(Arrays.toString(row));
        }
    }

    private static List<String[]> readCsv(String filePath) throws IOException {
        List<String[]> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                data.add(values);
            }
        }
        return data;
    }

    private static List<String[]> sortMergeJoin(List<String[]> table1, List<String[]> table2) {
        List<String[]> result = new ArrayList<>();
        int i = 0, j = 0;

        while (i < table1.size() && j < table2.size()) {
            String[] row1 = table1.get(i);
            String[] row2 = table2.get(j);
            int compare = row1[0].compareTo(row2[0]);

            if (compare < 0) {
                // Row in table1 has a smaller key, move to the next row in table1
                i++;
            } else if (compare > 0) {
                // Row in table2 has a smaller key, move to the next row in table2
                j++;
            } else {
                // Keys match, find the range of matching keys in both tables
                int iStart = i, jStart = j;

                while (i < table1.size() && table1.get(i)[0].equals(row1[0])) {
                    i++;
                }
                // i now points to the first non-matching row in table1 or the end of the table

                while (j < table2.size() && table2.get(j)[0].equals(row2[0])) {
                    j++;
                }
                // j now points to the first non-matching row in table2 or the end of the table

                // Cross join all matching keys
                for (int iMatch = iStart; iMatch < i; iMatch++) {
                    for (int jMatch = jStart; jMatch < j; jMatch++) {
                        String[] combinedRow = new String[table1.get(iMatch).length + table2.get(jMatch).length - 1];
                        System.arraycopy(table1.get(iMatch), 0, combinedRow, 0, table1.get(iMatch).length);
                        System.arraycopy(table2.get(jMatch), 1, combinedRow, table1.get(iMatch).length, table2.get(jMatch).length - 1);
                        result.add(combinedRow);
                    }
                }
            }
        }

        return result;
    }
}
