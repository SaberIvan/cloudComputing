# Project Overview

This project comprises two distinct parts: a Cloud Computing Assignment focusing on the use of Spark RDD API for text analysis in cloud environments, and a RAKE Implementation Report that applies the Rapid Automatic Keyword Extraction (RAKE) algorithm for keyword extraction in natural language processing (NLP). Each part contributes to the understanding and practical application of cloud computing and NLP methodologies.

## Table of Contents

- [Cloud Computing Assignment (COMP5349)](#cloud-computing-assignment-comp5349)
- [RAKE Implementation Report](#rake-implementation-report)
- [Deployment on AWS EC2 with YARN Cluster](#deployment-on-aws-ec2-with-yarn-cluster)
- [Conclusion](#conclusion)

## Cloud Computing Assignment (COMP5349)

### Introduction

Designed by Dr. Ying Zhou, this assignment challenges students to extract keywords from legal documents using the Spark RDD API. It utilizes a dataset adapted from the Contract Understanding Atticus Dataset (CUAD), comprising over 13,000 clauses from 510 legal contracts.

### Task Overview

Tasked with implementing an extraction algorithm using basic Spark RDD API operations to analyze "Governing Law", "Change of Control", and "Anti-assignment" clauses. The goal is to extract the top 20 keywords from these categories.

### Deliverables

Submissions include a Jupyter notebook with the implemented solution, a README file with execution instructions on Google Colab, and a detailed report on the implementation's data flow and RDD usage.

## RAKE Implementation

*Algorithm 1 primarily uses the flatMap() process to compute the score of each keyword, selecting the top four scores as extracted keywords. This selection implies that these words' extracted document frequency (edf) is marked as 1, while the edf for other words in the phrase is marked as 0. Each phrase division into keywords marks its reference document frequency (rdf) as 1, showcasing an efficient use of space. Following the flatMap() process, the aggregateByKey() API calculates the total edf and rdf values, introducing a new rdf parameter through automatic addition. The final step involves ranking the essentiality of keywords using the sortBy() function and listing the top 20 keywords. A unique approach is applied to manage special cases within the "Change of Control" and "Anti-assignment" corpus by filtering out single characters that may lead to inaccurately high scores.

Algorithm 2 engages in a flatMap() process to dissect the parse into single words within their apparent keywords, adopting a time-for-space strategy. This step is followed by calculating single word scores to determine the keywords' scores. By grouping by single word, a tuple of a single word with its keywords list is created, aiding in score calculation based on the frequency and the total number of words among all keywords. The subsequent step involves joining the flatMap() result with a score map containing single words, thus deriving a map of keywords with varying single word scores. The reduceByKey() function is then used to aggregate these scores, ranking the keywords and selecting the top 20. To meet the assignment's requirements, the rdd result is converted into a list using the collect function, and the top 20 elements from these lists are placed into a data frame for further processing and display.*




# How to run this jupyter file

Open the google drive (url = https://drive.google.com/drive/my-drive), sign in with your own google account.T hen clicking the "My Drive" button which is under the search bar and upload the .ipynb file. Right click the file you uploaded, find"open way" then choose "Google colaboratory" . Then the jupyter file is opened already.

## Upload .csv data files

For this assignment, there are two data files called "Governing_Law.csv" and "Anti_assignment_CIC_g3.csv". As the picture show below, click on the red box button and choose these two files. ![Example position of how to upload data files](fileUpload.png)
After uploading, the file will appear in the sidebar.

## Run each cell through "run cell" button(As the picture shows below)

![Run](run.png)


