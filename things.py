#Example of closure

def outer (a):
    b = "variable in outer()"
    def inner (c):
        print(a, b, c)
    return inner

# Now the return value from outer() can be saved for later
func = outer ("test")
func (1) # prints "test variable in outer() 1