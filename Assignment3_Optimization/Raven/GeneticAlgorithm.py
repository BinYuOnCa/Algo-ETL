import numpy
import ga

"""
Genetic Algorithm:

The target (y) is to maximize this equation:
    y = w1x1+w2x2+w3x3+w4x4+w5x5+6wx6
    where (x1,x2,x3,x4,x5,x6)=(5, -2.3, 9.6, 7.1, -10, -1.7)
    What are the best values for the 6 weights w1 to w6?
    We are going to use the genetic algorithm for the best possible values after a number of generations.
"""

# Inputs of the equation.
equation_inputs = [5, -2.3, 9.6, 7.1, -10, -1.7]

# Number of the weights we are looking to optimize.
num_weights = len(equation_inputs)


# Genetic algorithm parameters:
# Population size:
sol_per_pop = 8
# Mating pool size:
num_parents_mating = 4

# Defining the population size.
pop_size = (sol_per_pop,num_weights)
# Creating the weight.
weight = numpy.random.uniform(low=-1.0, high=1.0, size=pop_size)
print(weight)

best_outputs = []
num_generations = 10
for generation in range(num_generations):
    print("Generation : ", generation)
    # Measuring the fitness of each chromosome in the population.
    fitness = ga.cal_pop_fitness(equation_inputs, weight)
    print("Fitness")
    print(fitness)

    best_outputs.append(numpy.max(numpy.sum(weight * equation_inputs, axis=1)))
    # The best result in the current iteration.
    print("Best result : ", numpy.max(numpy.sum(weight * equation_inputs, axis=1)))

    # Selecting the best parents in the population for mating.
    parents = ga.select_mating_pool(weight, fitness,
                                    num_parents_mating)
    print("Parents")
    print(parents)

    # Generating next generation using crossover.
    offspring_crossover = ga.crossover(parents,
                                       offspring_size=(pop_size[0] - parents.shape[0], num_weights))
    print("Crossover")
    print(offspring_crossover)

    # Adding some variations to the offspring using mutation.
    # Default mutation number is 1. In this case, we use 2.
    offspring_mutation = ga.mutation(offspring_crossover, num_mutations=2)
    print("Mutation")
    print(offspring_mutation)

    # Creating the new population based on the parents and offspring.
    weight[0:parents.shape[0], :] = parents
    weight[parents.shape[0]:, :] = offspring_mutation

# Getting the best solution after iterating finishing all generations.
# At first, the fitness is calculated for each solution in the final generation.
fitness = ga.cal_pop_fitness(equation_inputs, weight)
# Then return the index of that solution corresponding to the best fitness.
best_match_idx = numpy.where(fitness == numpy.max(fitness))

print("Best solution : ", weight[best_match_idx, :])
print("Best solution fitness : ", fitness[best_match_idx])

import matplotlib.pyplot

matplotlib.pyplot.plot(best_outputs)
matplotlib.pyplot.xlabel("Iteration")
matplotlib.pyplot.ylabel("Fitness")
matplotlib.pyplot.show()