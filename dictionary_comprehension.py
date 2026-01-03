names = ["Bruce", "Clark", "Peter", "Logan", "Wade"]
heros = ["Batman", "Superman", "Spiderman", "Wolverine", "Deadpool"]
print (zip(names, heros))

# my_dict = {}
# for name, hero in zip(names, heros):
#     my_dict[name] = hero
# print (my_dict)

# Dict comprehension
my_dict = {name:hero for name, hero in zip(names, heros)}
print (my_dict)

# Using enumerate with zip
"""
enumerate() takes 1 iterable and pairs each value with its position index
"""
students = ['An', 'Bob', 'Cindy', 'Kai']
grades = [92, 98, 67, 85]

for i, (student, grade) in enumerate(zip(students, grades)):
    print(i, student, grade)