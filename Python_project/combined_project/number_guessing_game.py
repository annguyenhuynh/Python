import random

top_of_range = int(input("Type a number: "))

#--The number must be greater than 0
if top_of_range <= 0:
    print("Please type a number greater than 0")
    quit()



# With randrange, the upperbound is NOT included
# To include upper bound in the range, use randint instead
random_number = random.randint(0,top_of_range)

guesses = 0

while True:
    guesses += 1
    user_guess = int(input("Make a guess: "))

    if user_guess <= 0:
        print("Please type a number greater than 0")

    if user_guess == random_number:
        print("You got it right")
        break
    elif user_guess > random_number:
        print("You are above the number")
    else:
        print("You are below the number")

print("You got it in", guesses, "guesses")
    
