# local vs global scope
# local scope
def say_hello():
    name = "Alice" 
    print(f"Hello, {name}")
say_hello()

#global scope
name2 = "Alice" 
def say_hello2():
    print(f"Hello, {name2}")
say_hello2()

var = say_hello2()
age = int(input("How old are you? "))

message = print(f"{var} you said you are {age} years old")

# version 2 
name2 = str(input("What is your name? ")) 
def say_hello2():
    return f"Hello, {name2}"
var = say_hello2() 
age = int(input("How old are you? "))
print(f"{var} you said you are {age} years old")  # ← el print va aquí afuera
