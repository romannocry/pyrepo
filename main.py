from utils.typing import *

@type_check_decorator
def example_function(num:str, text:list):
    print(f"Number: {num}, Text: {text}")



# Example usage
example_function("42", [42])  # This will work fine
# example_function("42", "Hello")  # Uncommenting this line will raise a TypeError


@type_check_decorator
def exponential(startingValue:int,increase:float,yearCount:int):
    return startingValue*(1+increase)**yearCount


print(exponential(1,0.2,10))


word = "howareyou"

word2 = word[::-1]

print(word2)

arr = [1,2,3,4]
print(arr[::-1])
print(arr[0::-1])

print("yo")

print("this is a test")
