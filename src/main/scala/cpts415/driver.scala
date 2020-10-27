package cpts415

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.graphframes.GraphFrame

object driver {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def main(args: Array[String]) {
    /**
     * Start the Spark session
     */
    val spark = SparkSession
      .builder()
      .appName("cpts415-bigdata")
      .config("spark.some.config.option", "some-value")//.master("local[*]")
      .getOrCreate()
    spark.sparkContext.addFile("https://raw.githubusercontent.com/DataOceanLab/CPTS-415-Project-Template/main/data/users.csv")
    spark.sparkContext.addFile("https://raw.githubusercontent.com/DataOceanLab/CPTS-415-Project-Template/main/data/relationships.csv")

    /**
     * Load CSV file into Spark SQL
     */
    // You can replace the path in .csv() to any local path or HDFS path
    var users = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("users.csv"))
    var relationships = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("relationships.csv"))
    users.createOrReplaceTempView("users")
    relationships.createOrReplaceTempView("relationships")
    users.show()
    relationships.show()

    /**
     * The basic Spark SQL functions
     */
    var new_users = spark.sql(
      """
        |SELECT *
        |FROM users
        |WHERE age = 30
        |""".stripMargin)
    new_users.show()
    new_users.write.mode(SaveMode.Overwrite).options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("data/new-users.csv")

    /**
     * The GraphFrame function.
     */

    // Create a Vertex DataFrame with unique ID column "id"
    val v = users
    // Create an Edge DataFrame with "src" and "dst" columns
    val e = relationships
    // Create a GraphFrame
    val g = GraphFrame(v, e)

    /**
     * Pattern matching: https://graphframes.github.io/graphframes/docs/_site/user-guide.html#subgraphs
     */
    // Select a path based on edges "e" of type "follow"
    // pointing from a younger user "a" to an older user "b".
    val paths = { g.find("(a)-[e]->(b)")
      .filter("e.relationship = 'follow'")
      .filter("a.age < b.age") }
    val e2 = paths.select("e.*")
    e2.show()

    // Construct the subgraph
    val g2 = GraphFrame(g.vertices, e2)

    /**
     * Common graph queries
     */
    // Query: Get in-degree of each vertex.
    g.inDegrees.show()

    // Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    /**
     * Run PageRank algorithm, and show results. https://graphframes.github.io/graphframes/docs/_site/quick-start.html
     */
    val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
    results.vertices.select("id", "pagerank").show()

    // Shortest path using GraphFrames (only show distance, in terms of num hops). Ignore the relationship attribute.
    // https://graphframes.github.io/graphframes/docs/_site/user-guide.html#shortest-paths
    val shortest_path = g.shortestPaths.landmarks(Seq("a")).run()
    shortest_path.select("id", "distances").show()

    /**
     * DO NOT RECOMMEND. Shortest path using GraphX (only show distance): https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api
     */

    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
    GraphGenerators.logNormalGraph(spark.sparkContext, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))

    /**
     * Stop the Spark session
     */
    spark.stop()
  }
}
