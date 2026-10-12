import random
for s in range(50):
    r = random.Random(s)
    draws = [r.random() < 0.5 for _ in range(20)]
    losses = draws[0::5]
    if sum(losses) == 1:
        print(s, losses)
