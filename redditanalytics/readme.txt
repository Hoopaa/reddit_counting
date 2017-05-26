To clean:

	$ mvn clean
	
To compile:

	$ mvn compile
	
To create jar:

	$ mvn package
	
To create eclipse project:

	$ mvn eclipse:eclipse

To run using maven (only for standalone):

	$ mvn exec:java -Dexec.mainClass="heigvd.bda.labs.wordcount.RedditAnalytics" -Dexec.args="NUM_REDUCERS INPUT_PATH OUTPUT_PATH"

To run using eclipse (only for standalone):

	Run Configuration -> New Java Application -> Define the main class and arguments
				
To run using Hadoop:

	$ ${HAOOP_HOME}/hadoop/bin jar target/redditanalytics-1.0-SNAPSHOT.jar heigvd.bda.labs.redditanalytics.RedditAnalytics NUM_REDUCERS INPUT_PATH OUTPUT_PATH

 
