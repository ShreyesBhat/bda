print("Enter number of employees:")
n=as.integer(readline())
EmpID<-as.vector(n)
EmpName<-as.vector(n)
DOJ<-as.vector(n)
EmpCode<-as.vector(n)
Dept<-as.vector(n)
Desig<-as.vector(n)
for(i in 1:n){
  print(paste("Enter details of Employee ", i))
  EmpID[i]=as.character(readline())
  EmpName[i]=as.character(readline())
  DOJ[i]=as.character(readline())
  EmpCode[i]=as.character(readline())
  Dept[i]=as.character(readline())
  Desig[i]=as.character(readline())
}

emp <- data.frame(EmpID, EmpName, DOJ, EmpCode, Dept, Desig)
print(emp)

write.csv(emp, file="l4.csv")

read.csv("l4.csv", row.names = 1)

x = data.frame("24", "Jaff", "16/09/28", "emp3", "net", "CRO")
write.table(x, file="l4.csv", append = TRUE, sep = ",", quote = FALSE, col.names = FALSE, row.names = FALSE)
read.csv("l4.csv")