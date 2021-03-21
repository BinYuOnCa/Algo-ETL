import numpy as np
from bitarray import bitarray
import random


def func_y(x):
    return (x-10000)**2


def check_if_valid_x_code(x_code):
    x = decoder(x_code)
    return x >= 0 and x < 256**3


def encoder(x):
    ret = bitarray()
    ret.frombytes(int(x).to_bytes(3, 'big'))
    return ret


def decoder(x_code):
    return int.from_bytes(x_code.tobytes(), 'big')


def generate_x0(population):
    return np.random.randint(0, 256**3, population)


def mate_selection(X, Y, size):
    X_rank = Y.argsort().argsort()
    X_rank_reverse = len(X_rank) - X_rank
    X_rank_weights = [2*r for i, r in enumerate(X_rank_reverse)]
    X_father_idx = random.choices(range(len(X)), weights=X_rank_weights, k=size)
    X_mother_idx = random.choices(range(len(X)), weights=X_rank_weights, k=size)
    X_father = np.take(X, X_father_idx)
    X_mother = np.take(X, X_mother_idx)
    return X_father, X_mother


def new_generation(parents, mix_range_pct, mutation_pct):
    def mix(code1, code2, mix_range_pct):
        length = len(code1)
        switch_idx = max(int(length/2 + mix_range_pct*length/100.0
                         * (np.random.rand() - 0.5)), 0)
        return code1[:switch_idx]+code2[switch_idx:]

    def mutate(code, mutation_pct):
        new_code = code.copy()
        n_mutate = int(len(code) * mutation_pct / 100.0)
        idx_array = np.random.randint(len(code), size=n_mutate)
        for i in idx_array:
            new_code[i] = not new_code[1]
        return new_code

    def generate(code1, code2, mix_range_pct, mutation_pct):
        while (True):
            new_code_mix = mix(code1, code2, mix_range_pct)

            # if child is same as one of its parents, mutation pct is set to 50
            if new_code_mix == code1 or new_code_mix == code2:
                new_code = mutate(new_code_mix, 50)
            else:
                new_code = mutate(new_code_mix, mutation_pct)
            if check_if_valid_x_code(new_code):
                return new_code

    X_father, X_mother = parents
    X_father_code = list(map(encoder, X_father))
    X_mother_code = list(map(encoder, X_mother))
    X_child_code = [generate(father, mother, mix_range_pct, mutation_pct)
                    for father, mother in zip(X_father_code, X_mother_code)]
    return np.array(list(map(decoder, X_child_code)))


def ga(population, iteration, mix_range_pct, mutation_pct):
    def run_one_generation(X, Y):
        parents = mate_selection(X, Y, population)
        new_X = new_generation(parents, mix_range_pct, mutation_pct)
        new_Y = np.vectorize(func_y)(new_X)
        y_min_idx = new_Y.argmin()
        x_min = new_X[y_min_idx]
        y_min = new_Y[y_min_idx]
        return new_X, new_Y, y_min_idx, x_min, y_min

    X = generate_x0(population)
    Y = np.vectorize(func_y)(X)
    min_idx = Y.argmin()
    x_min = X[min_idx]
    y_min = Y[min_idx]
    global_x_min, global_y_min = x_min, y_min
    print(f'>>>> start x_min; {x_min}, y_min: {y_min}')
    for i in range(iteration):
        X, Y, min_idx, x_min, y_min = run_one_generation(X, Y)
        if y_min < global_y_min:
            global_x_min, global_y_min = x_min, y_min
        print(f'>>>> {i:5d} x_min: {x_min}, y_min: {y_min},\t\
            global_x_min: {global_x_min}, global_y_min: {global_y_min}')


if __name__ == '__main__':
    ga(population=2000, iteration=100, mix_range_pct=20, mutation_pct=20)
