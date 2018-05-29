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
# FUNCTION my_filter
# ------------------------------------------
def my_filter(line,averageReviewPerCuisine,percentage_f):
  #Filter out any cuisines that have a number of reviews lower then the average and that has an amount of bad reviews above a given pesentage 
  totalReviews = line[1][0]
  negativeReviews = line[1][1]
  if totalReviews <= averageReviewPerCuisine:
    return False
  elif ((negativeReviews*100)/totalReviews)>=percentage_f:
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
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
  #1. Read dataset 
  inputRDD = sc.textFile(dataset_dir)
  
  #2. Parse json line to python dictionary 
  rawDataRDD = inputRDD.map(lambda line: json.loads(line))
  rawDataRDD.persist()
  #3. Parse dataset to tuple that contains a key of cuisene and a value of review data 
  parsedRDD = rawDataRDD.map(my_parse)
  
  #4. Group by cuisne key 
  groupedRDD = parsedRDD.groupBy(lambda line: line[0])
  
  #5. Parse to get total number of reviews, total number of negative reviews and total number of points per cuisne
  formattedRDD = groupedRDD.map(my_format)
  #5.1. Persis formattedRDD for future use
  formattedRDD.persist()
  
  #6. Calcualte the average number of reviews per cuisine
  #6.1. Count total number of reviews
  totalNumReviews  = rawDataRDD.count()
  #6.2. Count total number of cuisines 
  totalNumCuisines = formattedRDD.count()
  #6.3. Devide to get average number of reviews per cuisine 
  averageReviewPerCuisine = totalNumReviews/totalNumCuisines
 
  #7. Filter out non-valid cuisines
  filteredRDD = formattedRDD.filter(lambda line: my_filter(line,averageReviewPerCuisine,percentage_f))
  
  #8. Sort by Quality 
  #8.1. Call map to make the quality value for each cuisine
  qualityRDD = filteredRDD.map(my_quality)
  #8.2. Call sort on the quality value and sort in decending order
  sortedRDD = qualityRDD.sortBy(lambda x: x[1][3],ascending=False)
  sortedRDD.persist()
  
  #9. Save results to text files
  sortedRDD.saveAsTextFile(result_dir)
  #Debug code
  res = sortedRDD.take(10)
  for item in res:
    print(item)
   
  pass

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 5. We call to our main function
    my_main(source_dir, result_dir, percentage_f)
