# methods

student = {
    "name" : "Andres",
    "last_name" : "Betancur",
    "age" : 31,
    "grade" : "A"
}

# extract keys
print(student.keys())
# extract values
print(student.values())
# get one value from key
print(student.get("name"))
#remove an item
student.pop("grade")
print(student)
# get all key-values in dictionary format
print(student.items())

# loop through a distionary

for key, value in student.items():
    print(f"{key} : {value}")