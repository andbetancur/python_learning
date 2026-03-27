# basic calculator

num1 = float(input("Enter your first number: "))
num2 = float(input("Enter your second number: "))
operator = input("Enter an operator: (+, -, *, /, **): ")
result = float(0)

if operator == "+":
    result = num1 + num2
elif operator == "-":
    result = num1 - num2
elif operator == "*":
    result = num1 * num2
elif operator == "/":
    if num2 == 0:
        result = "Error"
    else:
        result = num1/num2
elif operator == "**":
    result = num1 ** num2
else:
    result = "Operation not supported"
result = str(result)
print(f"The result is: {result}")

