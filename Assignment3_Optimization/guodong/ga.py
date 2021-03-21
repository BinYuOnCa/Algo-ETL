import matplotlib.pyplot as plt
import numpy as np
import abc

DNA_SIZE = 20  # DNA size
CROSS_RATE = 0.1
MUTATE_RATE = 0.02
POP_SIZE = 500
N_GENERATIONS = 500
X_BOUND = [-50, 50]  # x upper and lower bounds


class GA(object, metaclass=abc.ABCMeta):
    def __init__(self, DNA_size, cross_rate, mutation_rate, pop_size, ):
        self.DNA_size = DNA_size
        self.cross_rate = cross_rate
        self.mutate_rate = mutation_rate
        self.pop_size = pop_size

        self.pop = np.vstack([np.random.permutation(DNA_size) for _ in range(pop_size)])

    @abc.abstractmethod
    def translateDNA(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_fitness(self):
        raise NotImplementedError

    def select(self, fitness):
        """
        nature selection wrt pop's fitness
        :param fitness:
        :return:
        """
        idx = np.random.choice(np.arange(self.pop_size), size=self.pop_size, replace=True, p=fitness / fitness.sum())
        return self.pop[idx]

    @abc.abstractmethod
    def crossover(self, parent, pop):
        raise NotImplementedError

    def mutate(self, child):
        for point in range(self.DNA_size):
            if np.random.rand() < self.mutate_rate:
                swap_point = np.random.randint(0, self.DNA_size)
                swapA, swapB = child[point], child[swap_point]
                child[point], child[swap_point] = swapB, swapA
        return child

    def evolve(self, fitness):
        pop = self.select(fitness)
        pop_copy = pop.copy()
        for parent in pop:  # for every parent
            child = self.crossover(parent, pop_copy)
            child = self.mutate(child)
            parent[:] = child
        self.pop = pop


class F_GA(GA):
    def get_fitness(self, pred):
        return pred + 1e-3 - np.min(pred)

    def translateDNA(self, pop):
        """
        convert binary DNA to decimal and normalize it to a range of X_BOUND
        :param pop:
        :return:
        """
        return pop.dot(2 ** np.arange(DNA_SIZE)[::-1]) / float(2 ** DNA_SIZE - 1) * X_BOUND[1]

    def crossover(self, parent, pop):
        """
        mating process (genes crossover)
        :param parent:  the parents
        :param pop:  the population
        :return: new parent
        """
        if np.random.rand() < CROSS_RATE:
            i_ = np.random.randint(0, POP_SIZE, size=1)  # select another individual from pop
            cross_points = np.random.randint(0, 2, size=DNA_SIZE).astype(np.bool)  # choose crossover points
            parent[cross_points] = pop[i_, cross_points]  # mating and produce one child
        return parent




if __name__ == '__main__':
    ga = F_GA(DNA_size=DNA_SIZE, cross_rate=CROSS_RATE, mutation_rate=MUTATE_RATE, pop_size=POP_SIZE)

    for generation in range(N_GENERATIONS):
        pred = ga.translateDNA(ga.pop)
        fitness = ga.get_fitness(pred)
        ga.evolve(fitness)
        best_idx = np.argmax(fitness)
        print('Gen:', generation, '-> best fit: %.2f' % fitness[best_idx], )
 

