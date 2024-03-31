package com.arjunsk.sort_merge.inmemory;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ExternalMergeSort {
    private static final int CHUNK_SIZE = 2; // Adjust based on available memory

    public static void main(String[] args) throws IOException {
        String inputFilePath = "/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/left_input_unsorted.csv"; // Adjust path to your input file
        String outputFilePath = "/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/left_input_sorted.csv"; // Adjust path to your output file
        externalMergeSort(inputFilePath, outputFilePath);
    }

    public static void externalMergeSort(String inputFilePath, String outputFilePath) throws IOException {
        List<Path> sortedChunkFiles = splitAndSortChunks(inputFilePath);
        mergeSortedFiles(sortedChunkFiles, outputFilePath);

        // Cleanup: Delete the temporary sorted chunk files
        for (Path path : sortedChunkFiles) {
            Files.deleteIfExists(path);
        }
    }

    private static List<Path> splitAndSortChunks(String filePath) throws IOException {
        List<Path> chunkFiles = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            boolean endOfFile = false;
            while (!endOfFile) {
                List<String> lines = new ArrayList<>(CHUNK_SIZE);
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    String line = reader.readLine();
                    if (line == null) {
                        endOfFile = true;
                        break;
                    }
                    lines.add(line);
                }
                if (!lines.isEmpty()) {
                    Path chunkFile = sortAndSave(lines);
                    chunkFiles.add(chunkFile);
                }
            }
        }
        return chunkFiles;
    }

    private static Path sortAndSave(List<String> lines) throws IOException {
        Collections.sort(lines);
        Path chunkFile = Files.createTempFile("sorted_chunk_", ".txt");
        Files.write(chunkFile, lines);
        return chunkFile;
    }

    private static void mergeSortedFiles(List<Path> sortedFiles, String outputFilePath) throws IOException {
        PriorityQueue<BufferedReaderWrapper> queue = new PriorityQueue<>();
        for (Path sortedFile : sortedFiles) {
            BufferedReader reader = Files.newBufferedReader(sortedFile);
            String line = reader.readLine();
            if (line != null) {
                queue.add(new BufferedReaderWrapper(reader, line));
            }
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath))) {
            while (!queue.isEmpty()) {
                BufferedReaderWrapper brw = queue.poll();
                writer.write(brw.currentLine);
                writer.newLine();
                String nextLine = brw.reader.readLine();
                if (nextLine != null) {
                    brw.currentLine = nextLine;
                    queue.add(brw);
                } else {
                    brw.reader.close();
                }
            }
        }
    }

    static class BufferedReaderWrapper implements Comparable<BufferedReaderWrapper> {
        BufferedReader reader;
        String currentLine;

        public BufferedReaderWrapper(BufferedReader reader, String currentLine) {
            this.reader = reader;
            this.currentLine = currentLine;
        }

        @Override
        public int compareTo(BufferedReaderWrapper other) {
            return this.currentLine.compareTo(other.currentLine);
        }
    }
}
