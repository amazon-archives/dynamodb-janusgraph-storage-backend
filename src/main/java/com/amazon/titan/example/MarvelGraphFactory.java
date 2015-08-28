/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.titan.example;

import java.io.InputStreamReader;
import java.net.URL;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.Vertex;

/**
 *
 * @author Matthew Sowders
 *
 */
public class MarvelGraphFactory {
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int BATCH_SIZE = 10;
    private static final Logger LOG = LoggerFactory.getLogger(MarvelGraphFactory.class);
    public static final String APPEARED = "appeared";
    public static final String COMIC_BOOK = "comic-book";
    public static final String CHARACTER = "character";
    public static final String WEAPON = "weapon";
    public static final MetricRegistry REGISTRY = MetricManager.INSTANCE.getRegistry();
    public static final ConsoleReporter REPORTER = ConsoleReporter.forRegistry(REGISTRY).build();
    private static final String TIMER_COMMIT = "MarvelGraphFactory.commit";
    private static final String TIMER_LINE = "MarvelGraphFactory.line";
    private static final String TIMER_CREATE = "MarvelGraphFactory.create_";
    private static final String COUNTER_GET = "MarvelGraphFactory.get_";
    private static final String[] WEAPONS = { "claws", "ring", "shield", "robotic suit", "cards", "surf board", "glider", "gun", "swords", "lasso" };
    private static final AtomicInteger COMPLETED_TASK_COUNT = new AtomicInteger(0);
    private static final int POOL_SIZE = 10;

    public static void load(final TitanGraph graph, final int rowsToLoad, final boolean report) throws Exception {

        TitanManagement mgmt = graph.getManagementSystem();
        if (mgmt.getGraphIndex(CHARACTER) == null) {
            final PropertyKey characterKey = mgmt.makePropertyKey(CHARACTER).dataType(String.class).make();
            mgmt.buildIndex(CHARACTER, Vertex.class).addKey(characterKey).unique().buildCompositeIndex();
        }
        if (mgmt.getGraphIndex(COMIC_BOOK) == null) {
            final PropertyKey comicBookKey = mgmt.makePropertyKey(COMIC_BOOK).dataType(String.class).make();
            mgmt.buildIndex(COMIC_BOOK, Vertex.class).addKey(comicBookKey).unique().buildCompositeIndex();
            mgmt.makePropertyKey(WEAPON).dataType(String.class).make();
            mgmt.makeEdgeLabel(APPEARED).multiplicity(Multiplicity.MULTI).make();
        }
        mgmt.commit();

        ClassLoader classLoader = MarvelGraphFactory.class.getClassLoader();
        URL resource = classLoader.getResource("META-INF/marvel.csv");
        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
        int line = 0;
        Map<String, Set<String>> comicToCharacter = new HashMap<>();
        Map<String, Set<String>> characterToComic = new HashMap<>();
        Set<String> characters = new HashSet<>();
        BlockingQueue<Runnable> creationQueue = new LinkedBlockingQueue<>();
        try (CSVReader reader = new CSVReader(new InputStreamReader(resource.openStream()))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null && line < rowsToLoad) {
                line++;
                String comicBook = nextLine[1];
                String[] characterNames = nextLine[0].split("/");
                if (!comicToCharacter.containsKey(comicBook)) {
                    comicToCharacter.put(comicBook, new HashSet<String>());
                }
                List<String> comicCharacters = Arrays.asList(characterNames);
                comicToCharacter.get(comicBook).addAll(comicCharacters);
                characters.addAll(comicCharacters);

            }
        }

        for (String character : characters) {
            creationQueue.add(new CharacterCreationCommand(graph, character));
        }

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        for (String comicBook : comicToCharacter.keySet()) {
            creationQueue.add(new ComicBookCreationCommand(graph, comicBook));
            Set<String> comicCharacters = comicToCharacter.get(comicBook);
            for (String character : comicCharacters) {
                AppearedCommand lineCommand = new AppearedCommand(graph, new Appeared(character, comicBook));
                queue.add(lineCommand);
                if (!characterToComic.containsKey(character)) {
                    characterToComic.put(character, new HashSet<String>());
                }
                characterToComic.get(character).add(comicBook);
            }
            REGISTRY.histogram("histogram.comic-to-character").update(comicCharacters.size());
        }

        int maxAppearances = 0;
        String maxCharacter = "";
        for (String character : characterToComic.keySet()) {
            Set<String> comicBookSet = characterToComic.get(character);
            int numberOfAppearances = comicBookSet.size();
            REGISTRY.histogram("histogram.character-to-comic").update(numberOfAppearances);
            if (numberOfAppearances > maxAppearances) {
                maxCharacter = character;
                maxAppearances = numberOfAppearances;
            }
        }
        LOG.info("Character {} has most appearances at {}", maxCharacter, maxAppearances);

        for (int i = 0; i < POOL_SIZE; i++) {
            executor.execute(new BatchCommand(graph, creationQueue));
        }
        executor.shutdown();
        while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            LOG.info("Awaiting:" + queue.size());
            if(report) {
                REPORTER.report();
            }
        }

        executor = Executors.newSingleThreadExecutor();
        executor.execute(new BatchCommand(graph, queue));

        executor.shutdown();
        while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            LOG.info("Awaiting:" + queue.size());
            if(report) {
                REPORTER.report();
            }
        }
        LOG.info("MarvelGraphFactory.load complete");
    }

    public static class Appeared {
        final String character;
        final String comicBook;

        public Appeared(String character, String comicBook) {
            this.character = character;
            this.comicBook = comicBook;
        }

        public String getCharacter() {
            return character;
        }

        public String getComicBook() {
            return comicBook;
        }
    }

    public static class BatchCommand implements Runnable {
        final TitanGraph graph;
        final BlockingQueue<Runnable> commands;

        public BatchCommand(TitanGraph graph, BlockingQueue<Runnable> commands) {
            this.commands = commands;
            this.graph = graph;
        }

        @Override
        public void run() {
            int i = 0;
            Runnable command = null;
            while ((command = commands.poll()) != null) {
                try {
                    command.run();
                } catch (Throwable e) {
                    Throwable rootCause = ExceptionUtils.getRootCause(e);
                    String rootCauseMessage = null == rootCause ? "" : rootCause.getMessage();
                    LOG.error("Error processing comic book {} {}", e.getMessage(), rootCauseMessage, e);
                }
                if (i++ % BATCH_SIZE == 0) {
                    try {
                        commit(graph, commands);
                    } catch (Throwable e) {
                        LOG.error("error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
                    }
                }

            }
            try {
                commit(graph, commands);
            } catch (Throwable e) {
                LOG.error("error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
            }
        }

    }

    public static class ComicBookCreationCommand implements Runnable {
        final String comicBook;
        final TitanGraph graph;

        public ComicBookCreationCommand(final TitanGraph graph, final String comicBook) {
            this.comicBook = comicBook;
            this.graph = graph;
        }

        @Override
        public void run() {
            createComicBook(graph, comicBook);
        }

        private static Vertex createComicBook(TitanGraph graph, String value) {
            long start = System.currentTimeMillis();

            Vertex vertex = graph.addVertex(null);
            vertex.setProperty(COMIC_BOOK, value);
            REGISTRY.counter(COUNTER_GET + COMIC_BOOK).inc();
            long end = System.currentTimeMillis();
            long time = end - start;
            REGISTRY.timer(TIMER_CREATE + COMIC_BOOK).update(time, TimeUnit.MILLISECONDS);
            return vertex;
        }

    }

    public static class CharacterCreationCommand implements Runnable {
        final String character;
        final TitanGraph graph;

        public CharacterCreationCommand(final TitanGraph graph, final String character) {
            this.character = character;
            this.graph = graph;
        }

        @Override
        public void run() {
            createCharacter(graph, character);
        }

        private static Vertex createCharacter(TitanGraph graph, String value) {
            long start = System.currentTimeMillis();

            Vertex vertex = graph.addVertex(null);
            vertex.setProperty(CHARACTER, value);
            // only sets weapon on character vertex on initial creation.
            vertex.setProperty(WEAPON, WEAPONS[RANDOM.nextInt(WEAPONS.length)]);
            REGISTRY.counter(COUNTER_GET + CHARACTER).inc();
            long end = System.currentTimeMillis();
            long time = end - start;
            REGISTRY.timer(TIMER_CREATE + CHARACTER).update(time, TimeUnit.MILLISECONDS);
            return vertex;
        }
    }

    public static class AppearedCommand implements Runnable {
        final TitanGraph graph;
        final Appeared appeared;

        public AppearedCommand(final TitanGraph graph, Appeared appeared) {
            this.graph = graph;
            this.appeared = appeared;
        }

        @Override
        public void run() {
            try {
                processLine(graph, appeared);
            } catch (Throwable e) {
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                String rootCauseMessage = null == rootCause ? "" : rootCause.getMessage();
                LOG.error("Error processing line {} {}", e.getMessage(), rootCauseMessage, e);
            } finally {
                COMPLETED_TASK_COUNT.incrementAndGet();
            }
        }

    }

    protected static void processLine(final TitanGraph graph, Appeared appeared) {
        long start = System.currentTimeMillis();
        process(graph, appeared);
        long end = System.currentTimeMillis();
        long time = end - start;
        REGISTRY.timer(TIMER_LINE).update(time, TimeUnit.MILLISECONDS);
    }

    private static void commit(TitanGraph graph, BlockingQueue<Runnable> commands) {
        long start = System.currentTimeMillis();
        graph.commit();
        long end = System.currentTimeMillis();
        long time = end - start;
        REGISTRY.timer(TIMER_COMMIT).update(time, TimeUnit.MILLISECONDS);
    }

    private static void process(TitanGraph graph, Appeared appeared) {
        Vertex comicBookVertex = get(graph, COMIC_BOOK, appeared.getComicBook());
        if (null == comicBookVertex) {
            REGISTRY.counter("error.missingComicBook." + appeared.getComicBook()).inc();
            comicBookVertex = graph.addVertex(null);
            comicBookVertex.setProperty(COMIC_BOOK, appeared.getComicBook());
        }
        Vertex characterVertex = get(graph, CHARACTER, appeared.getCharacter());
        if (null == characterVertex) {
            REGISTRY.counter("error.missingCharacter." + appeared.getCharacter()).inc();
            characterVertex = graph.addVertex(null);
            characterVertex.setProperty(CHARACTER, appeared.getCharacter());
            // only sets weapon on character vertex on initial creation.
            characterVertex.setProperty(WEAPON, WEAPONS[RANDOM.nextInt(WEAPONS.length)]);
        }
        characterVertex.addEdge(APPEARED, comicBookVertex);
    }

    private static Vertex get(TitanGraph graph, String key, String value) {
        Iterator<Vertex> it = graph.getVertices(key, value).iterator();
        Vertex vertex = null;
        if (it.hasNext()) {
            vertex = it.next();
        }
        return vertex;

    }
}
