li = [9,1,8,2,7,3,6,4,5]
s_li = sorted(li,reverse=True)
print(s_li)

# Sort values without creating new object
li.sort() #Sort this list in place, so it will return None
print("Sorted list:", li)

tup = (9,1,8,2,7,3,4,6,5)
s_tup = sorted(tup)
print('Tuple\t', s_tup)

list = [-6,-5,-4,1,2,3]
s_list = sorted(list, key=abs)
print(s_list)

class Employee():
    def __init__(self, name, age, salary):
        self.name = name
        self.age = age
        self.salary = salary
    
    def __repr__(self):
        return '({},{},{})'.format(self.name, self.age, self.salary)

from operator import attrgetter

e1 = Employee("Carl", 37, 70000)
e2 = Employee("Sarah", 29, 80000)
e3 = Employee("John", 43, 90000)

employees = [e1, e2, e3]

s_employees = sorted(employees, key=attrgetter('age'))
s_employees_1 = sorted(employees,key = lambda e: e.salary)
print(s_employees)
print(s_employees_1)