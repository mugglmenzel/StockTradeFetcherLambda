name := "StockTradeFetcherLambda"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.37"
libraryDependencies += "com.amazonaws" % "aws-lambda-java-core" % "1.1.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"