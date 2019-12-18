print("Enter number of students:")
n=as.integer(readline())
usn = as.vector(n)
name = as.vector(n)
marks = as.vector(n)
for(i in 1:n){
  print(paste("Enter USN, Name and Marks of ",i))
  usn[i]=as.character(readline())
  name[i]=as.character(readline())
  marks[i]=as.integer(readline())
}
student <- data.frame(usn,name,marks)
print(student)

n=readline()
age=as.vector(n)
for(i in 1:n){
  print(paste("Enter age of ",i))
  age[i]=as.integer(readline())
}
x <- data.frame(usn, name, marks, age)
print(x)
summary(x)

range1 <- subset(x, marks > 25 & name=="Jaffar")
print(range1)