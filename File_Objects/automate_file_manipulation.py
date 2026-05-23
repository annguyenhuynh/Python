import os

#os.chdir('path/to/file') 
os.chdir ('/Users/AnhHuynh/Documents/FALL 2023/CS50/PYTHON')
print(os.getcwd()) # --> print out the path above

# Print all the files in the directory
for f in os.listdir():
  file_name, file_ext = os.path.splitext(f)
  # print(file_name)

  if '-' in file_name:
    f_title, f_num = file_name.split('-', 1) # the second argument is maxsplit, which determines how mny times to split on the same delimiter
    
    file_ext = file_ext.strip()
    f_title = f_title.strip()
    f_num = f_num.strip()[1:].zfill(2) #zfill function is used for patching numbers(in this case, adding 0 in front of the number. This is to avoid 1 and 10 to be placed next to each other)
    # print('{}-{}{}'.format(f_num, f_title,file_ext))

    new_name = '{}-{}{}'.format(f_num, f_title,file_ext)
    os.rename(f,new_name)

  
  


