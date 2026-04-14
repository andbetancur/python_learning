# writing to a file

""" with open("notes.txt", "w") as file:
    file.write("This is your first note\n")
    file.write("You are learning file handling in Python\n") """

# read from a file

with open("notes.txt", "r") as file:
    content = file.read()
    print('File content:\n', content)

# appending to file

with open("notes.txt", "a") as file:
    file.write("\nThis is an append note")