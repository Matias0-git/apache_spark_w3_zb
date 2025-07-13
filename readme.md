# Week 3: Zach bootcamp - Apache Spark

With this third publication I want to showcase what we did on week 3 of the zach bootcamp. I am going to cover the concepts that he taught us and the hands-on labs.

First of all, for the concepts that were introduced in week 3 are:

- Day 1
  - Architecture of apache spark
    - What spark is good at and what is not good at
    - How does spark work
  - The roles of the driver
  - The roles of the executor
  - How does it all come together
  - Types of joins in spark
  - How does shuffle work and how to minimize it at high volumes
  - Shuffle and skew problems and ways to deal with it
  - Sparks on databricks vs regular spark
  - How to properly understand spark query plans
  - How can spark read data
- Lab 1
  - Partitioning
  - Sorting
  - Data read/write operations
- Day 2
  - Differences between spark server & spark notebooks
  - PySpark and ScholarSpark usage scenarios
  - Implications of using UDFs
  - Is a PySpark UDFs inferior to ScholarSpark UDFs
  - Spark tuning
  - Which api to pikc based on the job
    - DataFrame API
    - Spark SQL API
    - RDD API
    - DataSet API
- Lab 2
  - Implemented custom logic with UDFs
  - Efficiently combine a very large dataset with a much smaller lookup table, avoiding the performance bottleneck of a full shuffle join
- Day 3
  - Unit testing and integration testing for spark pipelines
  - Emphasizing the benefits of catching errors early and where you can find them
    - How to cath bug in dev and in production
  - Picking up standards from software engineering to improve quality as a data engineer
  - Tradeoff between business velocity and sustainability
  - Principles of doing data engineering with a software engineering mindset
- Lab 3
  - Built unit tests and integration tests for the spark jobs created on the lab

# Book suggestion to complete the assignments of week 3

This particular week turned into two as the most complicated stuff for me was deeply figuring out what was the underlying stuff (under the hood) that apache did instead of just coding my way through labs and assignments. This may be just for me but I like to get a complete understanding of how things work and started as this may seem as a pretty nascent career but it has been for over 40+ years on the making since the start of HDFS and MapReduce:
- Hadoop: The definitive guide from Tom White
- Learning spark: Lighting-fast big data analysis from Holden Karau
- Spark: The definitive guide from Bill Chambers


# Assignments of week 3: [Assignments on zach's github](https://github.com/DataExpert-io/data-engineer-handbook/tree/main/bootcamp/materials/3-spark-fundamentals/homework)

- Task 1 -> Is on the archive named tasks_assignment_1 were we used a game data analysis given by zach, where our main focus was to optimize performance with different join strategies and aggregations
- Task 2 ->You can check out this task on the pair archives which are called: hosts_accumulated and user_devices each one has its own job and test.Here we converted two postgreSQL queries from week 2 into pyspark jobs with their own unit tests using small, fake input data and compare that to ensure correctness
