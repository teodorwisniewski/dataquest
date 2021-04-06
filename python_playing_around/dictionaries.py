
d = {1: "one",True:"True",0:"zero",False:"False"}
print(d)

print(f"check it out False:{hash(False)}")
print(f"check it out Siema:{hash('Siema')}")
d['Siema'] = 'Siema'

print(1 in d)
print("cos" in d)
print("End")
