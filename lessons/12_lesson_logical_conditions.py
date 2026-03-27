# logical conditions: use and, or and not operators
age = input("How old are you? ")
has_id = input("Do you have ID? ")
can_vote = "You Can Vote"

age = int(age)

if age >= 18 and has_id.lower() == "yes":
    can_vote
else:
    can_vote = "You can't vote"
print(f"The result for you to vote is: {can_vote}")


""" age = 25
has_id = True

if age >= 18 and has_id:
    print("You can vote")
else:
    print("You can't vote") """