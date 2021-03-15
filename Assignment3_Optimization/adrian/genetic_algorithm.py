import matplotlib.pyplot as plt
import math
import random

def main():
    pop_size = 500  # population size
    upper_limit = 100  # max_allowed value
    chromosome_length = 10  # binary bits
    iter = 500
    pc = 0.5  # probability of crossover
    pm = 0.02  # probability of mutation
    results = []
    pop = init_population(pop_size, chromosome_length)
    best_X = []
    best_Y = []
    for i in range(iter):
        obj_value = calc_obj_value(pop, chromosome_length, upper_limit)
        fit_value = calc_fit_value(obj_value)
        best_individual, best_fit = find_best(pop, fit_value)
        results.append([bin2dec(best_individual, upper_limit, chromosome_length), best_fit])
        selection(pop, fit_value)
        crossover(pop, pc)
        mutation(pop, pm)
        if iter % 20 == 0:
            best_X.append(results[-1][0])
            best_Y.append(results[-1][1])
    print(f"best x = {results[-1][0]}, fitness y = {results[-1][1]}")
    # Plot result points
    plt.scatter(best_X, best_Y, s=3, c='blue')
    X1 = [i / float(10) for i in range(0, 100, 1)]
    Y1 = [(x**2) for x in X1]
    plt.plot(X1, Y1)
    plt.show()
    # iterate curve
    # plot_iter_curve(iter, results)


def plot_obj_func():
    # testing function y = x**2
    X1 = [i / float(10) for i in range(0, 100, 1)]
    Y1 = [(x**2) for x in X1]
    plt.plot(X1, Y1)
    plt.show()


def plot_currnt_individual(X, Y):
    X1 = [i / float(10) for i in range(0, 100, 1)]
    Y1 = [(x**2) for x in X1]
    plt.plot(X1, Y1)
    plt.scatter(X, Y, c='r', s=5)
    plt.show()


def plot_iter_curve(iter, results):
    X = [i for i in range(iter)]
    Y = [results[i][1] for i in range(iter)]
    plt.plot(X, Y)
    plt.show()


def bin2dec(binary, upper_limit, chromosome_length):
    # convert binary to decimal
    t = 0
    for j in range(len(binary)):
        t += binary[j] * 2 ** j
    t = t * upper_limit / (2 ** chromosome_length - 1)
    return t


def init_population(pop_size, chromosome_length):
    # fill population with random list of 0s and 1s
    pop = [[random.randint(0, 1) for i in range(chromosome_length)] for j in range(pop_size)]
    return pop


def decode_chromosome(pop, chromosome_length, upper_limit):
    X = []
    for ele in pop:
        temp = 0
        for i, coff in enumerate(ele):
            temp += coff * (2 ** i)
        X.append(temp * upper_limit / (2 ** chromosome_length - 1))
    return X


def calc_obj_value(pop, chromosome_length, upper_limit):
    obj_value = []
    X = decode_chromosome(pop, chromosome_length, upper_limit)
    for x in X:
        obj_value.append(0 if x == 0 else 1/x**2)
    return obj_value


def calc_fit_value(obj_value):
    fit_value = []
    c_min = 0
    for value in obj_value:
        if value > c_min:
            temp = value
        else:
            temp = 0.
        fit_value.append(temp)
    return fit_value


def find_best(pop, fit_value):
    best_individual = []
    best_fit = fit_value[0]
    for i in range(1, len(pop)):
        if (fit_value[i] > best_fit):
            best_fit = fit_value[i]
            best_individual = pop[i]
    return best_individual, best_fit


def cum_sum(fit_value):
    # calculate the cumulative fitness probability
    temp = fit_value[:]
    for i in range(len(temp)):
        fit_value[i] = (sum(temp[:i + 1]))


def selection(pop, fit_value):
    # 轮赌法
    p_fit_value = []
    total_fit = sum(fit_value)
    for i in range(len(fit_value)):
        p_fit_value.append(fit_value[i] / total_fit)
    cum_sum(p_fit_value)
    pop_len = len(pop)
    # generate a sorted random probability series
    ms = sorted([random.random() for i in range(pop_len)])
    fitin = 0
    newin = 0
    newpop = pop[:]
    while newin < pop_len:
        if (ms[newin] < p_fit_value[fitin]):
            newpop[newin] = pop[fitin]
            newin = newin + 1
        else:
            fitin = fitin + 1
    pop = newpop[:]


def crossover(pop, pc):
    # crossover based on set probability
    pop_len = len(pop)
    for i in range(pop_len - 1):
        if (random.random() < pc):
            cpoint = random.randint(0, len(pop[0]))
            temp1 = []
            temp2 = []
            temp1.extend(pop[i][0:cpoint])
            temp1.extend(pop[i + 1][cpoint:len(pop[i])])
            temp2.extend(pop[i + 1][0:cpoint])
            temp2.extend(pop[i][cpoint:len(pop[i])])
            pop[i] = temp1[:]
            pop[i + 1] = temp2[:]


def mutation(pop, pm):
    # mutate based on set probability
    px = len(pop)
    py = len(pop[0])
    for i in range(px):
        if (random.random() < pm):
            mpoint = random.randint(0, py - 1)
            if (pop[i][mpoint] == 1):
                pop[i][mpoint] = 0
            else:
                pop[i][mpoint] = 1


if __name__ == '__main__':
    main()