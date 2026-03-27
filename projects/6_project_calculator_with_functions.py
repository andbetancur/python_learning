# calculator with functions

def get_bill_amount():
    return float(input("Enter the bill amount: $ "))

def get_tip_porcentage():
    return float(input("Enter the tip percentage (e.g., 10 for 10%): "))

def calculate_tip(bill_amount, tip_percentage):
    return (bill_amount * tip_percentage) / 100

def calculate_total(bill_amount, tip_amount):
    return bill_amount + tip_amount

def display_result(tip, total):
    print(f"\nTip: ${tip:.2f}")
    print(f"\nTotal Bill: ${total:.2f}")

bill_amount = get_bill_amount()
tip_percentage = get_tip_porcentage()

tip = calculate_tip(bill_amount, tip_percentage)
total = calculate_total(bill_amount, tip)

result = display_result(tip, total)
