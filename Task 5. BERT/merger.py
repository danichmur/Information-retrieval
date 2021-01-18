import pickle

letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'V', 'X', 'Y', 'Z']


docs = {}
nope = []

for l in letters:
	with open(f"{l.lower()}_docs.p", 'rb') as f:
		docs.update(pickle.load(f))
	with open(f"{l.lower()}_nope.p", 'rb') as f:
		nope.extend(pickle.load(f))


print(f'{len(docs)} docs loaded')
print(f'{len(nope)} nope found')
pickle.dump(docs, open(f"docs.p", "wb"))
pickle.dump(nope, open(f"nope.p", "wb"))