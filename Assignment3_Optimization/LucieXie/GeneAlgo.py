from numpy.random import randint
from numpy.random import rand



# objective function in 2D
# min x^2 - 2x + 3 s.t. -10 <= x1 <= 10
#                        -5 <= x2 <= 15
# bounds can be adjusted
def objective(x):
    return x[0]**2 + x[1]**2 - 2*x[0] - 2*x[1] + 3

# bitstring to numbers
def bin_to_int(bounds, n_bits, bitstr):
    val = list()
    largest = 2**n_bits
    for i in range(len(bounds)):
        start, end = i * n_bits, (i * n_bits)+n_bits
        substring = bitstr[start:end]
        integer = int(''.join([str(s) for s in substring]), 2)
        value = bounds[i][0] + (integer/largest) * \
            (bounds[i][1] - bounds[i][0])
        val.append(value)
    return val

# step 2: selecting parents
def selection(population, scores, k=3):
    selec_idx = randint(len(population))
    for idx in randint(0, len(population), k-1):
        if scores[idx] < scores[selec_idx]:
            selec_idx = idx
    return population[selec_idx]

# step 3: crossover 2 parents to create 2 children (single point crossover)
def crossover(p1, p2, r_cross):
    c1, c2 = p1.copy(), p2.copy()
    if rand() < cross_rate:
        parents = randint(1, len(p1)-2)
        c1 = p1[:parents] + p2[parents:]
        c2 = p2[:parents] + p1[parents:]
    return [c1, c2]

# step 4: mutation
def mutation(bitstr, mut_rate):
    for i in range(len(bitstr)):
        if rand() < mut_rate:
            bitstr[i] = 1 - bitstr[i]

# genetic algorithm
def genetic_algorithm(objective, bounds, n_bits, n_iter, n_pop, cross_rate, mut_rate):
    # step 1: initialization
    population = [randint(0, 2, n_bits*len(bounds)).tolist()
                  for i in range(n_pop)]
    best, best_eval = 0, objective(population[0])

    for gen in range(n_iter):
        val = [bin_to_int(bounds, n_bits, p) for p in population]
        scores = [objective(d) for d in val]

        for i in range(n_pop):
            if scores[i] < best_eval:
                best, best_eval = population[i], scores[i]
                print(">%d, current new best is f(%s) = %f" %
                      (gen,  val[i], scores[i]))

        selected = [selection(population, scores) for i in range(n_pop)]
        children = list()

        for i in range(0, n_pop, 2):
            p1, p2 = selected[i], selected[i+1]
            for c in crossover(p1, p2, cross_rate):
                mutation(c, mut_rate)
                children.append(c)
        population = children

    return [best, best_eval]


# range restriction
bounds = [[-10.0, 10.0], [-5.0, 15.0]]
# total iterations
n_iter = 200
# bits per variable
n_bits = 16
# population size
n_pop = 100
# crossover rate
cross_rate = 0.5
# mutation rate
mut_rate = 1.0 / (float(n_bits) * len(bounds))
# perform the genetic algorithm search
best, score = genetic_algorithm(
    objective, bounds, n_bits, n_iter, n_pop, cross_rate, mut_rate)
best_c = bin_to_int(bounds, n_bits, best)
print('Done! Best pair is f(%s) = %f' % (best_c, score))
