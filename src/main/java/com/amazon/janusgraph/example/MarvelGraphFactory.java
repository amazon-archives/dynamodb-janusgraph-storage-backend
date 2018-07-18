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

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.util.stats.MetricManager;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
@Slf4j
public final class MarvelGraphFactory {
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int BATCH_SIZE = 10;
    public static final String APPEARED = "appeared";
    public static final String COMIC_BOOK = "comic-book";
    public static final String CHARACTER = "character";
    public static final String WEAPON = "weapon";
    public static final MetricRegistry REGISTRY = MetricManager.INSTANCE.getRegistry();
    private static final ConsoleReporter REPORTER = ConsoleReporter.forRegistry(REGISTRY).build();
    private static final String TIMER_LINE = "MarvelGraph.line";
    private static final String TIMER_CREATE = "MarvelGraph.create_";
    private static final String COUNTER_GET = "MarvelGraph.get_";
    private static final List<String> WEAPONS = Lists.newArrayList("claws", "ring", "shield", "robotic suit", "cards", "surf board", "glider", "gun", "swords", "lasso");
    private static final AtomicInteger COMPLETED_TASK_COUNT = new AtomicInteger(0);
    private static final int POOL_SIZE = 10;
    private static final int TIMEOUT_SIXTY_SECONDS = 60;

    private MarvelGraphFactory() {
    }

    /**
     * Loads a graph with part or all of the marvel data.
     * @param graph the graph object to load with marvel data.
     * @param rowsToLoad the number of rows to load with data
     * @param report whether to report statistics or not
     * @throws IOException if there was a problem loading the marvel file
     * @throws InterruptedException if the JanusGraph loading was interrupted
     */
    public static void load(final JanusGraph graph, final int rowsToLoad, final boolean report)
        throws IOException, InterruptedException {

        final JanusGraphManagement mgmt = graph.openManagement();
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

        final ClassLoader classLoader = MarvelGraphFactory.class.getClassLoader();
        final URL resource = classLoader.getResource("META-INF/marvel.csv");
        Preconditions.checkNotNull(resource);
        final Map<String, Set<String>> comicToCharacter = new HashMap<>();
        final Map<String, Set<String>> characterToComic = new HashMap<>();
        final Set<String> characters = new HashSet<>();
        final BlockingQueue<Runnable> creationQueue = new LinkedBlockingQueue<>();
        try (CSVReader reader = new CSVReader(new InputStreamReader(resource.openStream(), Charset.forName("UTF-8")))) {
            reader.readAll().subList(0, rowsToLoad).forEach(nextLine -> {
                final String comicBook = nextLine[1];
                final String[] characterNames = nextLine[0].split("/");
                if (!comicToCharacter.containsKey(comicBook)) {
                    comicToCharacter.put(comicBook, new HashSet<>());
                }
                final List<String> comicCharacters = Arrays.asList(characterNames);
                comicToCharacter.get(comicBook).addAll(comicCharacters);
                characters.addAll(comicCharacters);
            });
        }
        creationQueue.addAll(characters.stream().map(character -> new CharacterCreationCommand(character, graph))
            .collect(Collectors.toList()));

        final BlockingQueue<Runnable> appearedQueue = new LinkedBlockingQueue<>();
        for (Map.Entry<String, Set<String>> entry : comicToCharacter.entrySet()) {
            final String comicBook = entry.getKey();
            final Set<String> comicCharacters = entry.getValue();
            creationQueue.add(new ComicBookCreationCommand(comicBook, graph));
            for (String character : comicCharacters) {
                final AppearedCommand lineCommand = new AppearedCommand(graph, new Appeared(character, comicBook));
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
        for (Map.Entry<String, Set<String>> entry : characterToComic.entrySet()) {
            final String character = entry.getKey();
            final Set<String> comicBookSet = entry.getValue();
            final int numberOfAppearances = comicBookSet.size();
            REGISTRY.histogram("histogram.character-to-comic").update(numberOfAppearances);
            if (numberOfAppearances > maxAppearances) {
                maxCharacter = character;
                maxAppearances = numberOfAppearances;
            }
        }
        log.info("Character {} has most appearances at {}", maxCharacter, maxAppearances);

        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            executor.execute(new BatchCommand(graph, creationQueue));
        }
        executor.shutdown();
        while (!executor.awaitTermination(TIMEOUT_SIXTY_SECONDS, TimeUnit.SECONDS)) {
            log.info("Awaiting:" + creationQueue.size());
            if (report) {
                REPORTER.report();
            }
        }

        executor = Executors.newSingleThreadExecutor();
        executor.execute(new BatchCommand(graph, appearedQueue));

        executor.shutdown();
        while (!executor.awaitTermination(TIMEOUT_SIXTY_SECONDS, TimeUnit.SECONDS)) {
            log.info("Awaiting:" + appearedQueue.size());
            if (report) {
                REPORTER.report();
            }
        }
        log.info("MarvelGraph.load complete");
    }

    @RequiredArgsConstructor
    @Getter
    private static class Appeared {
        private final String character;
        private final String comicBook;
    }

    @RequiredArgsConstructor
    private static class BatchCommand implements Runnable {
        private final JanusGraph graph;
        private final BlockingQueue<Runnable> commands;

        @Override
        public void run() {
            int i = 0;
            Runnable command = null;
            while ((command = commands.poll()) != null) {
                try {
                    command.run();
                } catch (Throwable e) {
                    final Throwable rootCause = ExceptionUtils.getRootCause(e);
                    final String rootCauseMessage = Optional.ofNullable(rootCause).map(Throwable::getMessage).orElse("");
                    log.error("Error processing comic book {} {}", e.getMessage(), rootCauseMessage, e);
                }
                if (i++ % BATCH_SIZE == 0) {
                    try {
                        graph.tx().commit();
                    } catch (Throwable e) {
                        log.error("error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
                    }
                }

            }
            try {
                graph.tx().commit();
            } catch (Throwable e) {
                log.error("error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
            }
        }

    }

    @RequiredArgsConstructor
    private static class ComicBookCreationCommand implements Runnable {
        private final String comicBook;
        private final JanusGraph graph;

        @Override
        public void run() {
            final long start = System.currentTimeMillis();
            final Vertex vertex = graph.addVertex();
            vertex.property(COMIC_BOOK, comicBook);
            REGISTRY.counter(COUNTER_GET + COMIC_BOOK).inc();
            final long end = System.currentTimeMillis();
            final long time = end - start;
            REGISTRY.timer(TIMER_CREATE + COMIC_BOOK).update(time, TimeUnit.MILLISECONDS);
        }
    }

    @RequiredArgsConstructor
    private static class CharacterCreationCommand implements Runnable {
        private final String character;
        private final JanusGraph graph;

        @Override
        public void run() {
            final long start = System.currentTimeMillis();
            final Vertex vertex = graph.addVertex();
            vertex.property(CHARACTER, character);
            // only sets weapon on character vertex on initial creation.
            vertex.property(WEAPON, WEAPONS.get(RANDOM.nextInt(WEAPONS.size())));
            REGISTRY.counter(COUNTER_GET + CHARACTER).inc();
            final long end = System.currentTimeMillis();
            final long time = end - start;
            REGISTRY.timer(TIMER_CREATE + CHARACTER).update(time, TimeUnit.MILLISECONDS);
        }
    }

    @RequiredArgsConstructor
    private static class AppearedCommand implements Runnable {
        private final JanusGraph graph;
        private final Appeared appeared;

        @Override
        public void run() {
            try {
                final long start = System.currentTimeMillis();
                process(graph, appeared);
                final long end = System.currentTimeMillis();
                final long time = end - start;
                REGISTRY.timer(TIMER_LINE).update(time, TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                final Throwable rootCause = ExceptionUtils.getRootCause(e);
                final String rootCauseMessage = Optional.ofNullable(rootCause.getMessage()).orElse("");
                log.error("Error processing line {} {}", e.getMessage(), rootCauseMessage, e);
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
            characterVertex.property(WEAPON, WEAPONS.get(RANDOM.nextInt(WEAPONS.size())));
        }
        characterVertex.addEdge(APPEARED, comicBookVertex);
    }

    private static Vertex get(final JanusGraph graph, final String key, final String value) {
        final GraphTraversalSource g = graph.traversal();
        final Iterator<Vertex> it = g.V().has(key, value);
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }
}
