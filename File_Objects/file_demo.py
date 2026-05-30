# ## Open file with Context manager

# with open('test.txt', 'r') as f:
#     for line in f:
#         print(line, end='')

# Write a file
"""Need to have a file open in write mode"""
# with open('test2.txt', 'w') as t:
#     t.write('Test')
#     t.seek(0)  #Overwrite based on location
#     t.write('R')

# Copy a file to another file
with open('test.txt', 'r') as rf:
    with open('test_copy.txt', 'w') as wf:
        for line in rf:
            wf.write(line)