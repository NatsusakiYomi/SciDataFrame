from timeit import timeit
import re

def find(string, text):
    if string.find(text) > -1:
        pass

def re_find(string, text):
    if re.match(text, string):
        pass

def best_find(string, text):
    if text in string:
       pass

def reverse_find(string, text):
    if string.rfind(text):
        pass

print(timeit("find(string, text)", "from __main__ import find; string='lookforme'; text='look'"))
print(timeit("re_find(string, text)", "from __main__ import re_find; string='lookforme'; text='me'"))
print(timeit("best_find(string, text)", "from __main__ import best_find; string='lookforme'; text='me'"))
print(timeit("reverse_find(string, text)", "from __main__ import reverse_find; string='lookforme'; text='look'"))