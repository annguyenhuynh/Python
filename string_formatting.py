person = {'name': 'An', 'age':28}
sent = 'My name is {} and I am {} years old'.format(person['name'], person['age'])
sent_1 = 'My name is {name} and I am {age} years old'.format(**person)
print(sent)
print(sent_1)

tag = 'h1'
text = 'This is a headline'
sentence = '<{0}>{1}</{0}>'.format(tag,text)
print(sentence)

l = ['Kim', 63]
mom_sent = 'My mom name is {0[0]} and my mom is {0[1]}'.format(l)
print(mom_sent)
# Why {0[0]} and {0[1]}? Since there is ONLY one object, it becomes one argument only.
# The [0] and [1] are the index of the attributes

class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age
p1= Person('Phong', 32)

hub_sent = 'My husband is {0.name} and he is {0.age} years old'.format(p1)
print(hub_sent)

Format number
for i in range (1,11):
    sentence = 'The value is {:02}'.format(i)
    print(sentence)

pi = 3.14152965
pi_sent = 'Pi is equal to {:.2f}'.format(pi)
print(pi_sent)

big_num = '1 MB is equal to {:,.2f} bytes'.format(1000**2)
print(big_num)

# Format dates
import datetime
my_date = datetime.datetime(2022, 9, 24, 12, 30, 45)

my_date_sent = '{:%B %d %Y}'.format(my_date)
print(my_date_sent)
