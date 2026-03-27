# conditinal statements
# if, elif and else

""" if condition:
    # logic
elif condition:
    # logic
else:
    # logic """

temperature = 16
message = ""

if temperature > 30:
    message = "Hot"
    print(f"It's hot, temperature: {temperature} Celsius")
elif temperature > 20:
    message = "Warm"
    print(f"It's warm, temperature: {temperature} Celsius")
elif temperature > 15:
    message = "Not cold yet"
    print(f"It's not cold yet, temperature: {temperature} Celsius")
else:
    message = "Cold"
    print(f"It's cold, temeprature: {temperature} Celsius")

print(f"It's {message} and the temperature is: {temperature} Celsius")