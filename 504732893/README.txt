This project was split such that we worked on different tasks throughout the project, and tried to split up the work based on making sure that we had every piece working.
This means we should have a decent understanding of each others parts, but also, helped each other when necessary to format our data properly for each step of the task.

Another important thing to note was that our model set threshold to 0.25 and all of our work was done using pyspark's spark dataframe library rather than formatting our
SQL queries the SQL style we were taught, though we still needed to understand the SQL concepts for making the right queries on the data.

We also used 2 variable flags that assumes models are already created and we don't need reads, or models aren't created and need to perform reads. This is mainly for assuming we 
don't want to keep reloading and recreating the parquets, but if these parquets already exist, we can just simply load the parquet file and move on from there to save time. 