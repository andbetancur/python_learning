# lopps
# while loop

""" while condition:
    # repetative code """

count = 1

while count <= 5:
    print("Count is: ", count)
    count += 1
    print(count)


# for loop

""" for i in range(start,stop):
    # repeat this block """

# range (1,6) that means start from 1 and stop before 6 (1,2,3,4,5)

for i in range(1,6):
    print("Number: ", i)

# loop thorough a list
fruits = ["apple", "banana", "cherry"]

for fruit in fruits:
    print(fruit.capitalize())
