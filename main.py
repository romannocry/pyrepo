from utils.typing import *

@type_check_decorator
def example_function(num:str, text:list):
    print(f"Number: {num}, Text: {text}")



# Example usage
example_function("42", [42])  # This will work fine
# example_function("42", "Hello")  # Uncommenting this line will raise a TypeError