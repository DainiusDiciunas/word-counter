package com.dainiusd.words_counter.counter;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

@Component
public class CounterService {

    private static final Logger LOG = LoggerFactory.getLogger(CounterService.class);

    private static final int EVERY_FIVE_SECONDS = 5 * 1000;
    private static final String UPLOAD_PATH = "src/uploads";
    private static final String COUNTED_PATH = "src/counted/";
    private static final String FILE_EXTENSION = ".txt";
    private static final String COUNTED_EXTENSION = ".counted";
    private static final String A_TO_G = "a-g";
    private static final String H_TO_N = "h-n";
    private static final String O_TO_U = "o-u";
    private static final String V_TO_Z = "v-z";

    @Scheduled(fixedDelay = EVERY_FIVE_SECONDS)
    public void countWords() {

        File folder = new File(UPLOAD_PATH);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles != null) {
            LOG.info("Found {} file(s)", listOfFiles.length);
            List<File> textFiles = stream(listOfFiles)
                    .filter(file -> file.getName().endsWith(".txt"))
                    .collect(Collectors.toList());
            LOG.info("Found text files {}", textFiles);
            textFiles.forEach(this::processFile);
        }
    }

    private void processFile(File file) {
        StringBuilder text = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(file.getPath()))) {
            stream.forEach(text::append);
            countAndWrite(text);
            File newName = new File(UPLOAD_PATH + "/" + removeExtension(file.getName()) + COUNTED_EXTENSION);
            file.renameTo(newName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void countAndWrite(StringBuilder text) {
        String reducedText = text.toString().toLowerCase().replaceAll("[’'`]", "");
        List<String> words = new ArrayList<>(asList(reducedText.split("\\W+")));
        Map<String, Integer> countedWords = countWords(words);
        Table<String, String, Integer> grouped = group(countedWords);
        grouped.rowMap().entrySet().forEach(this::writeToFile);
    }

    private void writeToFile(Entry<String, Map<String, Integer>> row) {
        Map<String, Integer> previouslyCountedWords = readExisting(row.getKey());
        Map<String, Integer> countedWords = row.getValue();
        if (!previouslyCountedWords.isEmpty()) {
            previouslyCountedWords.entrySet().forEach(entry -> merge(entry, countedWords));
        }
        List<String> lines = countedWords.entrySet().stream()
                .sorted(comparing(Entry::getKey))
                .map(entry -> entry.getKey() + " = " + entry.getValue())
                .collect(toList());
        try {
            Files.write(Paths.get(COUNTED_PATH + row.getKey() + FILE_EXTENSION), lines);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void merge(Entry<String, Integer> previousWord, Map<String, Integer> countedWords) {
        String word = previousWord.getKey();
        if (countedWords.containsKey(word)) {
            Integer count = countedWords.get(word);
            countedWords.replace(word, count, count + previousWord.getValue());
        } else {
            countedWords.put(word, previousWord.getValue());
        }
    }

    private Map<String, Integer> readExisting(String key) {
        Map<String, Integer> countedWords = new HashMap<>();
        try (Stream<String> stream = Files.lines(Paths.get(COUNTED_PATH + key + FILE_EXTENSION))) {
            stream.forEach(line -> toCountedWords(line, countedWords));
            return countedWords;
        } catch (IOException e) {
            return countedWords;
        }
    }

    private void toCountedWords(String line, Map<String, Integer> countedWords) {
        String word = substringBefore(line, " = ");
        String nm = substringAfter(line, " = ");
        countedWords.put(word, Integer.parseInt(nm));
    }

    private Table<String, String, Integer> group(Map<String, Integer> countedWords) {
        Table<String, String, Integer> table = HashBasedTable.create();
        countedWords.forEach((key, value) -> table.put(getInterval(key), key, value));
        return table;
    }

    private String getInterval(String word) {
        if (word.matches("^[" + A_TO_G + "].*$")) {
            return A_TO_G;
        } else if (word.matches("^[" + H_TO_N + "].*$")) {
            return H_TO_N;
        } else if (word.matches("^[" + O_TO_U + "].*$")) {
            return O_TO_U;
        } else {
            return V_TO_Z;
        }
    }

    private Map<String, Integer> countWords(List<String> words) {
        Map<String, Integer> map = new HashMap<>();
        for (String word : words) {
            if (map.containsKey(word)) {
                int count = map.get(word);
                map.put(word, count + 1);
            } else {
                map.put(word, 1);
            }
        }
        return map;
    }

}
