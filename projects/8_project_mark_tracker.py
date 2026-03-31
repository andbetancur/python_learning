# create a mark tracker
# project: Student Marks Tracker

students = {}

def show_menu():
    print("\nStudent Marks Tracker")
    print("1. Add Student")
    print("2. View Student")
    print("3. Update Marks")
    print("4. Delete Student")
    print("5. Exit Student")

def add_student():
    name = input("Enter student name: ")
    marks = float(input("Enter marks: "))
    students[name] = marks
    print(f"{name}'s marks added.")

#add_student()

def view_student():
    if not students:
        print("No students yet.")
    else:
        print("\nStudent Records: ")
        for name, mark in students.items():
            print(f"{name} : {mark}")

#view_student()

def update_marks():
    name = input("Enter the student name to update: ")
    if name in students:
        new_marks = float(input("Enter the new marks: "))
        students[name] = new_marks
        print(f"{name}'s marks updated.")
    else:
        print("Student not found")

#update_marks()

def delete_student():
    name = input("Enter the student name to delete: ") 
    if name in students:
        students.pop(name)
        print(f"{name} removed.")
    else:
        print("Student not found")

#delete_student()

# main lopp
while True:
    show_menu()
    choice = int(input("Choose an option: "))

    if choice == 1:
        add_student()
    elif choice == 2:
        view_student()
    elif choice == 3:
        update_marks()
    elif choice == 4:
        delete_student()
    elif choice == 5:
        print("Good bye")
        break
    else:
        print("Invalid choise, try again")