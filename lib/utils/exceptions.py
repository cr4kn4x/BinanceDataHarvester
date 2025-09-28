

def generate_invalid_arg_exception(arg: str, value): 
    return RuntimeError(f"unexpected value {value} provided for arg {arg}")