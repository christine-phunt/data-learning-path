# 5 — Not iterating directly over the elements of an iterator

list_of_fruits = ["apple", "pear", "orange"]

# bad practice

for i in range(len(list_of_fruits)):
    fruit = list_of_fruits[i]
    process_fruit(fruit)

# good practice

for fruit in list_of_fruits:
    process_fruit(fruit)
#########################

# 6 — Not using enumerate when you need the element and its index at the same time

list_of_fruits = ["apple", "pear", "orange"]

# bad practice

for i in range(len(list_of_fruits)):
    fruit = list_of_fruits[i]
    print(f"fruit number {i+1}: {fruit}")

# good practice

for i, fruit in enumerate(list_of_fruits):
    print(f"fruit number {i+1}: {fruit}")
#########################

# 7 — Not using zip to iterate over pairs of lists


list_of_letters = ["A", "B", "C"]
list_of_ids = [1, 2, 3]

# bad practice

for i in range(len(list_of_letters)):
    letter = list_of_letters[i]
    id_ = list_of_ids[i]
    process_letters(letter, id_)

# good practice

# list(zip(list_of_letters, list_of_ids)) = [("A", 1), ("B", 2), ("C", 3)]

for letter, id_ in zip(list_of_letters, list_of_ids):
    process_letters(letter, id_)
#########################

# 8 — Not using a context manager when reading or writing files

d = {"foo": 1}

# bad practice

f = open("./data.csv", "wb")
f.write("some data")

v = d["bar"]  # KeyError
# f.close() never executes which leads to memory issues

f.close()

# good practice

with open("./data.csv", "wb") as f:
    f.write("some data")
    v = d["bar"]
# python still executes f.close() even if the KeyError exception occurs
#########################


# 9 — Using in to check if an element is contained in a (large) list

# bad practice
list_of_letters = ["A", "B", "C", "A", "D", "B"]
check = "A" in list_of_letters

# good practice
set_of_letters = {"A", "B", "C", "D"}
check = "A" in set_of_letters
#########################

# 10 — Passing mutable default arguments to functions (i.e. an empty list)

# bad practice


def append_to(element, to=[]):
    to.append(element)
    return to


>> > my_list = append_to("a")
>> > print(my_list)
>> > ["a"]

>> > my_second_list = append_to("b")
>> > print(my_second_list)
>> > ["a", "b"]

# good practice


def append_to(element, to=None):
    if to is None:
        to = []
    to.append(element)
    return to
#########################


11 — Returning different types in a single function

# bad practice


def get_code(username):
    if username != "ahmed":
        return "Medium2021"
    else:
        return None


code = get_code("besbes")

# good practice: raise an exception and catch it


def get_code(username):
    if username != "ahmed":
        return "Medium2021"
    else:
        raise ValueError


try:
    secret_code = get_code("besbes")
    print("The secret code is {}".format(secret_code))
except ValueError:
    print("Wrong username.")
#########################

# 12 — Using while loops when simple for loops would do the trick

# bad practice

i = 0
while i < 5:
    i += 1
    some_processing(i)
    ...

# good practice

for i in range(5):
    some_processing(i)
    ...
#########################


# 13 — Using stacked and nested if statements

user = "Ahmed"
age = 30
job = "data scientist"

# bad practice

if age > 30:
    if user == "Ahmed":
        if job == "data scientist":
            access = True
        else:
            access = False

# good practice

access = age > 30 and user == "ahmed" and job == "data scientist"
#########################

# 15 — Not using get() to return default values from a dictionary

user_ids = {
    "John": 12,
    "Anna": 2,
    "Jack": 10
}

# bad practice

name = "Paul"

if name in user_ids:
    user_id = user_ids[name]
else:
    user_id = None

# good practice

user_id = user_ids.get(name, None)
#########################

# 16 — Using try/except blocks that don’t handle exceptions meaningfully

user_ids = {"John": 12, "Anna": 2, "Jack": 10}

user = "Paul"
# bad practice

try:
    user_id = user_ids[user]
except:
    pass

# good practice

try:
    user_id = user_ids[user]
except KeyError:
    print("user id not found")
#########################
