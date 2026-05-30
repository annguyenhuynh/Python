import os
from datetime import datetime

# Get current directory
print(os.getcwd())

# Get existing files in the directory
print(os.listdir())

# Make a directory with subfolders (tree structure, can use to make the path only)
os.makedirs('os_module/test')

os.removedirs('os_module/test')

print(os.stat('/Users/AnhHuynh/Python/advanced_python/poem.txt'))

mod_time = os.stat('/Users/AnhHuynh/Python/advanced_python/poem.txt').st_mtime
print(datetime.fromtimestamp(mod_time))

# Print of the tree structure of a folder

for dirpath, dirnames, filenames in os.walk('/Users/AnhHuynh/Python/advanced_python'):
    print('Current path', dirpath)
    print('Directories', dirnames)
    print('Files', filenames)
    print()

print(os.environ.get('HOME'))

file_path = os.path.join(os.environ.get('HOME'), 'test.txt')
print(file_path)

# Split path and extension
print(os.path.splitext('/tmp/test.txt'))

print(dir(os.path))