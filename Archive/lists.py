fruitList = ['apple', 'red apple', 'banana','green apple', 'melon']

#appleList = ['apple' in fruit for fruit in fruitList]
appleList = [x for x in fruitList if 'apple' in x]
#appleList = [x*2 for x in fruitList]

print(appleList)