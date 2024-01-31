from functools import wraps

def type_check_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Check the types of arguments based on type hints
        for arg, hint in zip(args, func.__annotations__.values()):
            if not isinstance(arg, hint):
                raise TypeError(
                    f"Argument {arg} should be of type {hint.__name__}")

        # Check the types of keyword arguments based on type hints
        for key, value in kwargs.items():
            hint = func.__annotations__.get(key)
            if hint and not isinstance(value, hint):
                raise TypeError(
                    f"Argument {key} should be of type {hint.__name__}")

        # If type checks pass, call the original function
        return func(*args, **kwargs)

    return wrapper
    
__all__ = ['type_check_decorator']
