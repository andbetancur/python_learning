# strings deifnition

message = "     Hello Python! I would like to learn Python"

print(message)

# upper method
print(message.upper())
# lowercase method
print(message.lower())
# stripe method: remove unwanted leading/space
print(message.strip())
# replace method
print(message.replace("Hello", "Hi"))
# startwith
print(message.startswith("     Hello"))
# endwith
print(message.endswith("!"))
# count: how many time a word or letter is in
print(message.count("Python"))
print(message.count("e"))