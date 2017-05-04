/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.example;

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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.util.stats.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import au.com.bytecode.opencsv.CSVReader;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
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
    private static final String TIMER_LINE = "MarvelGraphFactory.line";
    private static final String TIMER_CREATE = "MarvelGraphFactory.create_";
    private static final String COUNTER_GET = "MarvelGraphFactory.get_";
    private static final String[] WEAPONS = { "claws", "ring", "shield", "robotic suit", "cards", "surf board", "glider", "gun", "swords", "lasso" };
    private static final AtomicInteger COMPLETED_TASK_COUNT = new AtomicInteger(0);
    private static final int POOL_SIZE = 10;

    public static void load(final JanusGraph graph, final int rowsToLoad, final boolean report) throws Exception {

        JanusGraphManagement mgmt = graph.openManagement();
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
            creationQueue.add(new CharacterCreationCommand(character, graph));
        }

        BlockingQueue<Runnable> appearedQueue = new LinkedBlockingQueue<>();
        for (String comicBook : comicToCharacter.keySet()) {
            creationQueue.add(new ComicBookCreationCommand(comicBook, graph));
            Set<String> comicCharacters = comicToCharacter.get(comicBook);
            for (String character : comicCharacters) {
                AppearedCommand lineCommand = new AppearedCommand(graph, new Appeared(character, comicBook));
                appearedQueue.add(lineCommand);
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

        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            executor.execute(new BatchCommand(graph, creationQueue));
        }
        executor.shutdown();
        while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            LOG.info("Awaiting:" + creationQueue.size());
            if(report) {
                REPORTER.report();
            }
        }

        executor = Executors.newSingleThreadExecutor();
        executor.execute(new BatchCommand(graph, appearedQueue));

        executor.shutdown();
        while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            LOG.info("Awaiting:" + appearedQueue.size());
            if(report) {
                REPORTER.report();
            }
        }
        LOG.info("MarvelGraphFactory.load complete");
    }

    @RequiredArgsConstructor
    @Getter
    public static class Appeared {
        final String character;
        final String comicBook;
    }

    @RequiredArgsConstructor
    public static class BatchCommand implements Runnable {
        final JanusGraph graph;
        final BlockingQueue<Runnable> commands;

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
                    	graph.tx().commit();
                    } catch (Throwable e) {
                        LOG.error("error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
                    }
                }

            }
            try {
            	graph.tx().commit();
            } catch (Throwable e) {
                LOG.error("error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
            }
        }

    }

    @RequiredArgsConstructor
    public static class ComicBookCreationCommand implements Runnable {
        final String comicBook;
        final JanusGraph graph;

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            Vertex vertex = graph.addVertex();
            vertex.property(COMIC_BOOK, comicBook);
            REGISTRY.counter(COUNTER_GET + COMIC_BOOK).inc();
            long end = System.currentTimeMillis();
            long time = end - start;
            REGISTRY.timer(TIMER_CREATE + COMIC_BOOK).update(time, TimeUnit.MILLISECONDS);
        }
    }

    @RequiredArgsConstructor
    public static class CharacterCreationCommand implements Runnable {
        final String character;
        final JanusGraph graph;

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            Vertex vertex = graph.addVertex();
            vertex.property(CHARACTER, character);
            // only sets weapon on character vertex on initial creation.
            vertex.property(WEAPON, WEAPONS[RANDOM.nextInt(WEAPONS.length)]);
            REGISTRY.counter(COUNTER_GET + CHARACTER).inc();
            long end = System.currentTimeMillis();
            long time = end - start;
            REGISTRY.timer(TIMER_CREATE + CHARACTER).update(time, TimeUnit.MILLISECONDS);
        }
    }

    @RequiredArgsConstructor
    public static class AppearedCommand implements Runnable {
        final JanusGraph graph;
        final Appeared appeared;

        @Override
        public void run() {
            try {
                long start = System.currentTimeMillis();
                process(graph, appeared);
                long end = System.currentTimeMillis();
                long time = end - start;
                REGISTRY.timer(TIMER_LINE).update(time, TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                String rootCauseMessage = null == rootCause ? "" : rootCause.getMessage();
                LOG.error("Error processing line {} {}", e.getMessage(), rootCauseMessage, e);
            } finally {
                COMPLETED_TASK_COUNT.incrementAndGet();
            }
        }

    }

    private static void process(final JanusGraph graph, final Appeared appeared) {
        Vertex comicBookVertex = get(graph, COMIC_BOOK, appeared.getComicBook());
        if (null == comicBookVertex) {
            REGISTRY.counter("error.missingComicBook." + appeared.getComicBook()).inc();
            comicBookVertex = graph.addVertex();
            comicBookVertex.property(COMIC_BOOK, appeared.getComicBook());
        }
        Vertex characterVertex = get(graph, CHARACTER, appeared.getCharacter());
        if (null == characterVertex) {
            REGISTRY.counter("error.missingCharacter." + appeared.getCharacter()).inc();
            characterVertex = graph.addVertex();
            characterVertex.property(CHARACTER, appeared.getCharacter());
            characterVertex.property(WEAPON, WEAPONS[RANDOM.nextInt(WEAPONS.length)]);
        }
        characterVertex.addEdge(APPEARED, comicBookVertex);
    }

    private static Vertex get(final JanusGraph graph, final String key, final String value) {
        final GraphTraversalSource g = graph.traversal();
        final Iterator<Vertex> it = g.V().has(key, value);
        return it.hasNext() ? it.next() : null;
    }
}
