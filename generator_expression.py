"""
Generator Expression is a lazy version of list comprehension.
Instead of building the whole list in memory, it produces values one-by-one only when needed.
Use parentheses ()
"""
nums = [1,2,3,4,5,6,7,8,9,10]

def gen_func(nums):
    for n in nums:
        yield n*2

my_gen = gen_func(nums) 
my_gen_1 = (map(lambda n:n*n, filter(lambda n:n%2==0, nums)))
# Must iterate to get values
for i in my_gen:
    print (i)

for x in my_gen_1:
    print(x)

