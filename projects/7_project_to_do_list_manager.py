# list manager: to add tasks, view tasks and remove tasks

todo_list =[]

def show_menu():
    print("\nTo do List App")
    print("1. View Tasks")
    print("2. Add Tasks")
    print("3. Remove Tasks")
    print("4. Exit")

# view tasks
def view_task():
    if not todo_list:
        print("No tasks yet")
    else:
        print("#\nYour tasks: ")
        for i, task in enumerate(todo_list, 1):
            print(f"{i}. {task}")

# add tasks
def add_task():
    task= input("Enter a new task ")
    todo_list.append(task)
    print(f"{task} added")

# remove task
def remove_task():
    view_task()
    try:
        task_num = int(input("Enter the number of the task to remove "))
        removed = todo_list.pop(task_num-1)
        print(f"{removed} removed successfully")
    except (IndexError, ValueError):
        print("Invalid task number")
# main loop
while True:
    show_menu()
    choice = input("Choose an option: ")

    if choice == "1":
        view_task()
    elif choice == "2":
        add_task()
    elif choice == "3":
        remove_task()
    elif choice == "4":
        print("Good bye")
        break
    else:
        print("Invalid choice, try again")