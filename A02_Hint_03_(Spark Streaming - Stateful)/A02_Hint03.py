# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import time
from pyspark.streaming import StreamingContext
import json
from __future__ import division



# ------------------------------------------
# FUNCTION my_parse
# ------------------------------------------
def my_parse(line):
  #Take the cuisine, points and evaluation from the dictionary and put them into a tuple with another tuple in it
  cuisine = line["cuisine"]
  points = line["points"]
  evaluation = line["evaluation"]
  return (cuisine,(points,evaluation))
  

# ------------------------------------------
# FUNCTION my_format
# ------------------------------------------
def my_format(cuisineData):
  #Combine all the grouped cuisine reviews into 1 tuple that contains a summary of the reviews 
  numOfReviews=0
  numOfNegativeReviews=0
  totalPoints=0
  for data in cuisineData[1]:
    review = data[1]
    numOfReviews=numOfReviews+1
    if review[1] == "Negative":
      numOfNegativeReviews=numOfNegativeReviews+1
      totalPoints=totalPoints-review[0]
      continue 
    else:
      totalPoints=totalPoints+review[0]
  return (cuisineData[0], (numOfReviews, numOfNegativeReviews, totalPoints))
# ------------------------------------------
# FUNCTION my_fake_key
# ------------------------------------------
def my_fake_key(line):
  return ("fake",(line[0],line[1][0],line[1][1],line[1][2]))

# ------------------------------------------
# FUNCTION my_filter
# ------------------------------------------
def my_filter(line,percentage_f):
  #Filter out any cuisines that have a number of reviews lower then the average and that has an amount of bad reviews above a given pesentage 
  
  totalReviews = line[1][3]
  totalCuisines = line[1][4]
  averageReviewPerCuisine=totalReviews/totalCuisines
  reviewsForCusisine = line[1][0]
  negativeReviewsForCusisine = line[1][1]
  if reviewsForCusisine < averageReviewPerCuisine:
    return False
  elif ((negativeReviewsForCusisine*100)/reviewsForCusisine)>percentage_f:
    return False
  else:
    return True
# ------------------------------------------
# FUNCTION my_quality
# ------------------------------------------
def my_quality(cuisine):
  #Calculate the "quality" of a cuisine by deviding the amount of points it has over the amount of reviews it has 
  quality = cuisine[1][2]/cuisine[1][0]
  return (cuisine[0], (cuisine[1][0], cuisine[1][1], cuisine[1][2], quality ))



# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc, monitoring_dir, result_dir, percentage_f, window_duration, sliding_duration, time_step_interval):
  inputDStream = ssc.textFileStream(monitoring_dir)
  #1. Create a window to read in the quartly files 
  windowDStream = inputDStream.window(window_duration * time_step_interval, sliding_duration * time_step_interval)
  windowDStream.cache()
  #2. Read in the data from the files 
  dictionaryStream = windowDStream.map(lambda x: json.loads(x))
  
  #3. Parse the files data into the informaton we need
  parsedStream = dictionaryStream.map(my_parse)
 
  #4. Group by cuisne key 
  groupedStream = parsedStream.transform(lambda rdd: rdd.groupBy(lambda line: line[0]))

  #5. Parse to get total number of reviews, total number of negative reviews and total number of points per cuisne
  formattedStream = groupedStream.map(my_format)
  #6. Cache the formattedStream to be used in more then 1 operations
  formattedStream.cache()
  
  
  #7. Calcualte the average number of reviews per cuisine. To get the total number of reviews and the total number of cuisines a lot of work must be done
  #7.1 First we must make a Stream that is makde up of a tuple. Each tuple starts with the key "fake" and includes the values for that cuisine in it's value
  fakeKeyStream = formattedStream.map(my_fake_key)
  
  #7.2.1 We then count the total numbers of reviews and the total number of cusines by calling count on inputDStream and formattedStream. 
  #The reason why we could not do this normally is because when .count() is called on a stream it does not give back a single value, instead it is a stream.
  totalReviewCountStream = windowDStream.count()
  totalCuisineCountStream = formattedStream.count()
  
  #7.2.2 We then call a map on the counts to generate a tuple that has a key of "fake" and the valueing being the respective count value
  totalReviewMapStream = totalReviewCountStream.map(lambda x: ("fake",x))
  totalCuisineMapStream = totalCuisineCountStream.map(lambda x: ("fake",x))
  
  #7.2.3 We then proform a join on these two streams. As a result we will get a a tuple in the stream with the key "fake" and a tuple for the value that contains the total reviews and total cuisines 
  joinedTotalStream = totalReviewMapStream.join(totalCuisineMapStream)
  
  #7.3 We then join the total cusine and reviews tuple with the tuple containing the data for each cuisine 
  joinedCuisineDataTotalStream = joinedTotalStream.join(fakeKeyStream)
  
  
  #7.4 We then format the tuples to suit our needs
  formattedTotalStream = joinedCuisineDataTotalStream.map(lambda x: (x[1][1][0], (x[1][1][1], x[1][1][2], x[1][1][3], x[1][0][0], x[1][0][1])))
  #formattedTotalStream.pprint()
  #8. We then filter out any cuisine that does not match our filter parameters
  filteredStream = formattedTotalStream.filter(lambda line: my_filter(line, percentage_f))
  
  #9. Sort the results by quality
  #9.1 First we must add the quality metric to the data 
  qualityAddedStream = filteredStream.map(my_quality)
  
  #9.2 Then we sort the stream by quality
  sortedStream = qualityAddedStream.transform(lambda rdd: rdd.sortBy(lambda x: x[1][3],ascending=False))

  sortedStream.cache()
  sortedStream.saveAsTextFiles(result_dir)
  sortedStream.pprint()
  
  pass

# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(monitoring_dir,
               result_dir,
               max_micro_batches,
               time_step_interval,
               percentage_f,
               window_duration,
               sliding_duration):
    # 1. We create the new Spark Streaming context.
    # This is the main entry point for streaming functionality. It requires two parameters:
    # (*) The underlying SparkContext that it will use to process the data.
    # (**) A batch interval, specifying how often it will check for the arrival of new data,
    # so as to process it.
    ssc = StreamingContext(sc, time_step_interval)

    # 2. We configure the maximum amount of time the data is retained.
    # Think of it: If you have a SparkStreaming operating 24/7, the amount of data it is processing will
    # only grow. This is simply unaffordable!
    # Thus, this parameter sets maximum time duration past arrived data is still retained for:
    # Either being processed for first time.
    # Being processed again, for aggregation with new data.
    # After the timeout, the data is just released for garbage collection.

    # We set this to the maximum amount of micro-batches we allow before considering data
    # old and dumping it times the time_step_interval (in which each of these micro-batches will arrive).
    ssc.remember(max_micro_batches * time_step_interval)

    # 3. We model the ssc.
    # This is the main function of the Spark application:
    # On it we specify what do we want the SparkStreaming context to do once it receives data
    # (i.e., the full set of transformations and ouptut operations we want it to perform).
    my_model(ssc, monitoring_dir, result_dir, percentage_f, window_duration, sliding_duration, time_step_interval)

    # 4. We return the ssc configured and modelled.
    return ssc


# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(source_dir, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)
        if verbose == True:
            print(file_name)

        # 3.2. We look for the pattern name= to remove all useless info from the start
        lb_index = file_name.index("name=u'")
        file_name = file_name[(lb_index + 7):]

        # 3.3. We look for the pattern ') to remove all useless info from the end
        ub_index = file_name.index("',")
        file_name = file_name[:ub_index]

        # 3.4. We append the name to the list
        res.append(file_name)
        if verbose == True:
            print(file_name)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose):
    # 1. We get the names of the files on source_dir
    files = get_source_dir_file_names(source_dir, verbose)

    # 2. We simulate the dynamic arriving of such these files from source_dir to dataset_dir
    # (i.e, the files are moved one by one for each time period, simulating their generation).
    for file in files:
        # 2.1. We copy the file from source_dir to dataset_dir
        dbutils.fs.cp(source_dir + file, monitoring_dir + file, False)

        # 2.2. We wait the desired transfer_interval
        time.sleep(time_step_interval)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay
            ):
    # 1. We setup the Spark Streaming context
    # This sets up the computation that will be done when the system receives data.
    ssc = StreamingContext.getActiveOrCreate(checkpoint_dir,
                                             lambda: create_ssc(monitoring_dir,
                                                                result_dir,
                                                                max_micro_batches,
                                                                time_step_interval,
                                                                percentage_f,
                                                                window_duration,
                                                                sliding_duration
                                                                )
                                             )

    # 2. We start the Spark Streaming Context in the background to start receiving data.
    # Spark Streaming will start scheduling Spark jobs in a separate thread.

    # Very important: Please note a Streaming context can be started only once.
    # Moreover, it must be started only once we have fully specified what do we want it to do
    # when it receives data (i.e., the full set of transformations and ouptut operations we want it
    # to perform).
    ssc.start()

    # 3. As the jobs are done in a separate thread, to keep our application (this thread) from exiting,
    # we need to call awaitTermination to wait for the streaming computation to finish.
    ssc.awaitTerminationOrTimeout(time_step_interval)

    if (race_conditions_extra_delay == True):
        time.sleep((sliding_duration - 1) * time_step_interval)

        # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose)

    # 5. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the Spark Streaming Context
    ssc.stop(stopSparkContext=False)

    # 6. Extra security stop command: It acts directly over the Java Virtual Machine,
    # in case the Spark Streaming context was not fully stopped.

    # This is crucial to avoid a Spark application working on the background.
    # For example, Databricks, on its private version, charges per cluster nodes (virtual machines)
    # and hours of computation. If we, unintentionally, leave a Spark application working, we can
    # end up with an unexpected high bill.
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input source folder (static dataset),
    # monitoring folder (dynamic dataset simulation) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    monitoring_dir = "/FileStore/tables/A02/my_monitoring/"
    checkpoint_dir = "/FileStore/tables/A02/my_checkpoint/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 16

    # 3. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval =20

    # 4. We specify the maximum amount of micro-batches that we want to allow before considering data
    # old and dumping it.
    max_micro_batches = dataset_micro_batches + 1

    # 5. We configure verbosity during the program run
    verbose = False

    # 6. Extra input arguments
    percentage_f = 10
    window_duration = 4
    sliding_duration = 4

    # 6.4. RACE Conditions: Discussed above. Basically, in which moment of the sliding_window do I want to start.
    # This performs an extra delay at the start of the file transferred to sync SparkContext with file transferrence.
    race_conditions_extra_delay = True

    # 7. We remove the monitoring and output directories
    dbutils.fs.rm(monitoring_dir, True)
    dbutils.fs.rm(result_dir, True)
    dbutils.fs.rm(checkpoint_dir, True)

    # 8. We re-create them again
    dbutils.fs.mkdirs(monitoring_dir)
    dbutils.fs.mkdirs(result_dir)
    dbutils.fs.mkdirs(checkpoint_dir)

    # 9. We call to my_main
    my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay
            )