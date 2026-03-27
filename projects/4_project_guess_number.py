# number guessing game

import random

secret_number = random.randint(1, 10)

guess_number = 0

while guess_number != secret_number:
    guess_number = int(input("Guess a number between 1 and 10: "))

    if guess_number < secret_number:
        print("Too low")
    elif guess_number > secret_number:
        print("Too high")
    else:
        print("Correct! You guessed!")