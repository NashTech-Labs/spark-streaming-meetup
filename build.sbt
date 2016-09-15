name := """spark-streaming-example"""

version := "1.0"

scalaVersion := "2.11.8"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
                             "org.apache.spark" %% "spark-streaming" % "2.0.0",
                            "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0",
                             "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0",
                             "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))

                           )



