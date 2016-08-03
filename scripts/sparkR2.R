
# demonstration of sparkR
# what's here:
# @ Setting up (setting SPARK_HOME etc.)
# @ Initiation of local cluster (find out exactly what's happening under the hood)
# @ import a dataset, minor manipulation
# @ run both OLS and logistic regression

# envt setup
Sys.setenv(SPARK_HOME = "C:/stack/spark-1.6.2-bin-hadoop2.6") # <--- change this to connect to server
.libPaths(c(file.path(Sys.getenv("SPARK_HOME", "R", "lib")), .libPaths()))

library(SparkR)

# initialize local spark cluster and SQL context
sc <- sparkR.init(master = "local")
sqlContext <- sparkRSQL.init(sc)

# read local file
adult <- read.csv("data/adult.csv")

# create a data frame in sparkR
DF <- createDataFrame(sqlContext, adult)
head(DF)
str(DF)

# some minor manipulation
DF$income <- as.factor(DF$income) # <- is this necessary?

# Doesnt work:
#idx <- which(DF$education == " Bachelors")
#idx <- which(DF[,"education"] == " Bachelors")
#idx <- which(DF[,4] == " Bachelors")

# This works:
DF2 <- subset(DF, DF$education == " Bachelors")
dim(DF2) # takes awhile

# Doesnt work
#DF$income %>% as.character
#unclass(DF$age) %>% head

# Cross validation



# ====

