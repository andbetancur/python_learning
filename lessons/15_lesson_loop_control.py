# loop control statement

# break means exist from loop

for i in range(1,10):
    if (i==7):
        break
    print(i)

# continue skip current iteration

for i in range(1,10):
    if (i==7):
        continue
    print(i)

# pass means do nothing (placeholder)

for i in range(5):
    if (i==2):
        pass
    print(i)