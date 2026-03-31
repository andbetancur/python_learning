# list common methods

fruits = ["apple", "banana", "cherry"]
print(f"before: {fruits}")

# append(): to add items to the end of the list
fruits.append("orange")

print(f"after: {fruits}")

# remo(): remove specific item

fruits.remove("banana")

print(f"after 2: {fruits}")

# pop(): remove last item from the list

fruits.pop()

print(f"after 3: {fruits}")

# insert()_ insert item in a specific idex

fruits.insert(0, "grape")

print(f"after 4: {fruits}")

# sot(): sort list alphabetically or numerically

numbers = [42, 10, 5, 90]
numbers.sort()
fruits.sort()

print(numbers)
print(f"after 5: {fruits}")

# len(): number of items in a list

print(len(fruits))
print(len(numbers))



