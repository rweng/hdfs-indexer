# Requirements

- gradle 0.9-rc3
- curl
- hadoop (make sure you adjusted the maxFiles limit!!)

# Build Manager

The build manager of the thesis is gradle 0.9-rc3: http://http://gradle.org.
To start using this project download and install it. Under mac, its just:

    brew install --HEAD gradle

# IDE Setup

If you are using eclipse, clone the project in your workspace, prepare eclipse:

    git clone git@github.com:rweng/hdfs-indexer.git
    gradle eclipse

Then just import it as an existing project into eclipse.

For highlighting the .gradle build file, install the groovy Eclipse plugin(http://groovy.codehaus.org/Eclipse+Plugin).
The update-url is http://dist.springsource.org/release/GRECLIPSE/e3.6/
Then add .gradle with Groovy Editor to Preferences => General => Editors => File Associations

# Run

First generate the jar with

    gradle jar

Put the test-file in hadoop

    hadoop fs -put test.csv /test.csv
  
Now you can run it with

    hadoop jar build/libs/*.jar /test.csv
  
# Test

Run all unit-tests

    gradle test