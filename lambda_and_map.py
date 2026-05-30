# lambda: small, anonymous function
"""
lambda arguments: expression
"""
add = lambda a,b: a+b
print(add(2,4))

# map: applies a function to each element in an iterable
"""
map(function,iterable)
"""
nums = [1,2,3,4,5]
squares = map(lambda n: n*n, nums)
print(list(squares))

numbers = [1,2,3,4,5,6,7,8,9,10]
"""
filter(condition, iterable)
"""
odd_squares = list (
    map(lambda x: x*x, filter(lambda x:x%2!=0, numbers)))
print(odd_squares)

# double for loop
"""
I want a (letter, num) pair for each letter in 'abcd' and each number in '0123'
"""
my_list = [(letter, num) for letter in 'abcd' for num in range(4)]
print(my_list)