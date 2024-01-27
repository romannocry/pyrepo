fruits = ["red apple", "green apple", "apple", "banana", "strawberry"]

#for f in fruits:
    #print(f"I love this {f}")

filtered = [x for x in fruits if "apple" in x ]

filtered = [x+ ' magic' for x in fruits]




for f in filtered:
    print(f"I love this {f}")