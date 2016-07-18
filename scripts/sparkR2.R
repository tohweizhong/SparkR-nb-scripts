Sys.setenv(SPARK_HOME = "C:/stack/spark-1.6.2-bin-hadoop2.6") # <--- change this to connect to server
.libPaths(c(file.path(Sys.getenv("SPARK_HOME", "R", "lib")), .libPaths()))

library(SparkR)

sc <- sparkR.init(master = "local")
sqlContext <- sparkRSQL.init(sc)

# read local file
adult <- read.csv("Documents/R/adult.csv")

DF <- createDataFrame(sqlContext, adult)
head(DF)
str(DF)

DF$income <- as.factor(DF$income)

# ====


df <- createDataFrame(sqlContext, iris)
mod <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")
