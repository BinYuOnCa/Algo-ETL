import numpy as np


def convert_binary(array, domain):
    '''
    returns converted value in domain from a dna series array
    :param array: (np 1d array)
    :param domain: (list (Anyof Float Int) (Anyof Float Int))
    :return: (Anyof Float Int)
    '''
    convert_factor = 2**np.arange(len(array))[::-1]
    dna_raw = convert_factor @ array
    max_lim = sum(convert_factor)
    dna_norm = dna_raw / max_lim * (domain[1] - domain[0]) + domain[0]
    return dna_norm


def fit(pop, fn, domain, adjust=True):
    '''
    returns fitness by using the population of dna series and adjusts the
    fitness to non-negative value if adjust is True
    :param pop: (np 2d array)
    :param fn: (Function)
    :param domain: (list (Anyof Float Int) (Anyof Float Int))
    :param adjust: (Bool)
    :return: (np 1d array)
    '''
    preds = np.apply_along_axis(lambda val: fn(convert_binary(val, domain)), 1,
                                pop)
    fitness = preds - preds.min() if adjust else preds
    return fitness


def select(pop, size, fn, domain):
    '''
    returns the selected population of dna series with row of size
    :param pop: (np 2d array)
    :param size: (Nat)
    :param fn: (Function)
    :param domain: (list (Anyof Float Int) (Anyof Float Int))
    :return: (np 2d array)
    '''
    fitness = fit(pop, fn, domain)
    prob = fitness / sum(fitness) if fitness.all() else None
    ind = np.random.choice(len(pop), size=size, p=prob)
    return pop[ind]


def unif_crossover(person, pop):
    '''
    returns 2 kids from person and a random person in pop based
    on uniform crossover
    :param person: (np 1d array)
    :param pop: (np 2d array)
    :return: (np 2d array)
    '''
    mate_ind = np.random.randint(len(pop), size=1)
    cross_pts = np.random.randint(2, size=len(person)).astype('bool')
    kid1, kid2 = person.copy(), person.copy()
    kid1[cross_pts] = pop[mate_ind, cross_pts]
    kid2[np.invert(cross_pts)] = pop[mate_ind, np.invert(cross_pts)]

    return np.append(kid1, kid2).reshape(2, -1)


def mutate(kid, mutate_rate):
    '''
    mutates a one of a dna bit for kid with prob of mutate_rate
    :param kid: (np 1d array)
    :param mutate_rate: Float
    :return: (None)
    '''
    if np.random.rand(1) < mutate_rate:
        ind = np.random.randint(len(kid), size=1)
        kid[ind] = 1 - kid[ind]
