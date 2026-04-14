# Project: Login simulator with error handling for empty input or wrong password

correct_user = "admin"
correct_password = "python123"

try:
    user_name = input("Enter your username: ")
    password = input("Enter your password: ")

    if user_name == "" or password == "":
        raise ValueError("Username or password can not be empty")
    if user_name != correct_user or password != correct_password:
        raise Exception("Incorrect username or password")
    print("Login successful! Welcome, ", user_name)
except ValueError as ve:
    print(ve)
except Exception as e:
    print(e)
finally:
    print("Login session end. Try again")