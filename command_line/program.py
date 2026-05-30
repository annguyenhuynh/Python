import sys

# print(sys.argv)

def function1(file):
  print('First function')
  print(file)

def function2(file):
  print('Second function')
  print(file)


arguments = sys.argv[1:]

if len(arguments) != 2:
  print(f'ERROR: 2 argument expected, {len(arguments)} give')
  sys.exit()

option = arguments[0]
filename = arguments[1]

if option == 'f':
  function1(filename)
elif option == 's':
  function2(filename)
else:
  print(f'ERROR: option not available')