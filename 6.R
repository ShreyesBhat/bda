aq=read.csv("C:/Users/Administrator/Documents/R/AirQuality.csv")
dim(aq)

sapply(aq, class)

is.na(aq)

aq$Avg[is.na(aq $Avg)] <- mean(aq$Avg, na.rm = TRUE)
aq

x <- na.omit(aq)
print(x)