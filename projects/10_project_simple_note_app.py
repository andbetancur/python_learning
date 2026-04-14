
def save_note(note_text):
    with open("notes_2.txt", "a") as file:
        file.write(note_text + "\n")

print("Welcome to the simple note app!")

note = input("Type your note: ")

save_note(note)

print("Your note has been saved")