car = read.csv("C:/Users/Administrator/Documents/R/mtcars.csv")
print(car)

nrow(mtcars)
ncol(mtcars)

ca=0
cm=0
for(i in 0:nrow(mtcars))
  ifelse(mtcars[i,9]==1 , cm<-cm+1, ca<-ca+1)
if(ca > cm){
  print("Automatic") 
}else{
  print("Manual") 
}

plot(mtcars$hp, mtcars$wt, 'p')
attach(mtcars)

newmtc=data.frame(mpg, as.integer(cyl), disp, hp, drat, wt, qsec, as.integer(vs), as.integer(am), gear, carb)
lapply(newmtc, class)
print(newmtc)

count=0
for(i in 0:nrow(mtcars))
  ifelse(mtcars[i,2]<5, count<-count+1, ' ')
mtcars[2]<5
print(paste("Number of cases where cylinder is less than 5 is ", count))