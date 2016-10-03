package com.twitter.graphjet.demo;

import java.io.InputStream;
import java.io.File;
import java.util.zip.GZIPInputStream;
import java.io.FileInputStream; 
import java.io.BufferedReader; 
import java.io.InputStreamReader;
import com.twitter.graphjet.algorithms.PageRank;
import com.twitter.graphjet.bipartite.segment.IdentityEdgeTypeMask;
import com.twitter.graphjet.directed.OutIndexedPowerLawMultiSegmentDirectedGraph;
import com.twitter.graphjet.stats.NullStatsReceiver;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 *
 * Reads in a multi-line adjacency list from multiple files in a directory, where ids are of type T.
 * Does not check for duplicate edges or nodes.
 *
 *  In each file, a node and its neighbors is defined by the first line being that
 * node's id and its # of neighbors, followed by that number of ids on subsequent lines.
 * For example, when ids are Ints,
 *    241 3
 *    2
 *    4
 *    1
 *    53 1
 *    241
 *    ...
 * In this file, node 241 has 3 neighbors, namely 2, 4 and 1. Node 53 has 1 neighbor, 241.
 *
 */
public class PageRankDemo {
  private static class TwitterStreamReaderArgs {
    @Option(name = "-inputFile", metaVar = "[value]", usage = "maximum number of segments", required = true)
    String inputFile;

    @Option(name = "-maxSegments", metaVar = "[value]", usage = "maximum number of segments")
    int maxSegments = 15;

    @Option(name = "-maxEdgesPerSegment", metaVar = "[value]", usage = "maximum number of edges in each segment")
    int maxEdgesPerSegment = 5000000;

    @Option(name = "-leftSize", metaVar = "[value]", usage = "expected number of nodes on left side")
    int leftSize = 5000000;

    @Option(name = "-leftDegree", metaVar = "[value]", usage = "expected maximum degree on left side")
    int leftDegree = 5000000;

    @Option(name = "-leftPowerLawExponent", metaVar = "[value]", usage = "left side Power Law exponent")
    float leftPowerLawExponent = 2.0f;
  }

  public static boolean insertVertice(LongOpenHashSet ids, long id) {
    if (!ids.contains(id)) {
      ids.add(id);
      return true;
    }
    return false;
  }

  public static void main(String[] argv) throws Exception {
    final TwitterStreamReaderArgs args = new TwitterStreamReaderArgs();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(90));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return;
    }
    
    String graphPath = args.inputFile;

    OutIndexedPowerLawMultiSegmentDirectedGraph bigraph =  new OutIndexedPowerLawMultiSegmentDirectedGraph(args.maxSegments, args.maxEdgesPerSegment,
            args.leftSize, args.leftDegree, args.leftPowerLawExponent,
            new IdentityEdgeTypeMask(),
            new NullStatsReceiver());

    LongOpenHashSet vertices = new LongOpenHashSet();

    final AtomicLong max = new AtomicLong();

    long start = System.currentTimeMillis();

    final AtomicInteger edgeCounter = new AtomicInteger();
    Files.walk(Paths.get(graphPath)).forEach(filePath -> {
      final AtomicInteger from = new AtomicInteger();
      final AtomicInteger to = new AtomicInteger();
      final AtomicInteger counter = new AtomicInteger();

      if (Files.isRegularFile(filePath)) {
        try {
          InputStream inputStream = Files.newInputStream(filePath);
          GZIPInputStream gzip = new GZIPInputStream(inputStream);
          BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
          String line;
          while((line = br.readLine()) != null) {
            if (line.startsWith("#") || line.startsWith("twitter_rv.net")) continue;
            String[] tokens = line.split("\\s+");
//System.out.println(tokens[0] + " : " + tokens[1]);
            int cur;
            if (tokens.length > 1) {
              // new vertex
              cur = Integer.parseInt(tokens[0]);
              from.set(cur);
              to.set(Integer.parseInt(tokens[1]));
              bigraph.addEdge(from.get(), to.get(), (byte) 1);
              edgeCounter.incrementAndGet();
              if (insertVertice(vertices, cur)) {
                if (max.get() < cur) {
                   max.set(cur);
                }
              }
              if (insertVertice(vertices, to.get())) {
                if (max.get() < to.get()) {
                  max.set(to.get());
                }
              } 
            }
          }
        } catch (Exception e) {
          e.printStackTrace();	  
        }
      }
    });

    long numRuns = 10;
    long total = 0L;
    for (int i = 0; i < numRuns; ++i) {
      long loadedTime = System.currentTimeMillis();
      System.out.println("Running page rank.. # of vertices: " + vertices.size());

      PageRank pr = new PageRank(bigraph, vertices, max.get(), 0.85, 1e-15);
      double pagerank[] = pr.run(10);
      long endTime = System.currentTimeMillis();

      AtomicInteger constructedGraphEdgeCounter = new AtomicInteger();
      vertices.forEach(v -> {
        constructedGraphEdgeCounter.addAndGet(bigraph.getOutDegree(v));
        //System.out.println(v + " " + pagerank[(int)(long) v]);
      });
      if (edgeCounter.get() == constructedGraphEdgeCounter.get()) {
        System.out.println("Edge count " + edgeCounter.get());
      } else {
        System.err.println("Some of edges are dropped. Expected: " + edgeCounter.get() + " actual: " + constructedGraphEdgeCounter.get());
      }
      total += (endTime-loadedTime);
      System.out.println("PageRank took: " + (endTime-loadedTime) + " milliseconds ");
    }
    System.out.println("Average : " + (total / numRuns) + " milliseconds ");
  }
}
