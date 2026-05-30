nums = [1,1,2,1,3,4,3,4,5,5,6,7,8,7,9,9]
my_set = set()
for n in nums:
    my_set.add(n)
print (my_set)

# Set comprehension
set_test = {n for n in nums}
print (set_test)