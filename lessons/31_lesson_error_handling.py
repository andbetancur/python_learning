""" # how to handle common errors

age = int(input("Enter your age: "))
print(f"You are {age} years old")
print("You are " + age + "years old")

try:
    # code that may raise error
except ErrorType:
    # code to run if error happens """

try:
    num = int(input("Enter a number: "))
    print("Your Entered: ", num)
except (ValueError, TypeError):
    print("Please a valid number")


# custom valuerror
try:
    f = open("data.txt")
except FileNotFoundError:
    print("File not found")
finally:
    print("Execution finished.")

password = input("Enter password: ")
if password == "":
    raise ValueError("Password can't be empty.")