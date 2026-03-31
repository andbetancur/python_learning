# dictionaries contains keys an values (key-value pairs)

# creating dictionary
student = {
    "name" : "Andres",
    "last_name" : "Betancur",
    "age" : 31,
    "grade" : "A"
}

# accesing a dictionary

print(student["name"])
print(student.get("name"))
print(student["age"])
print(student["grade"])

full_name = student.get("name") + " " + student.get("last_name")
print(full_name)

# modifying a dictionary

student["age"] = 21 # modifying a value

student["major"] = "Math" # adding key-value

print(student)

