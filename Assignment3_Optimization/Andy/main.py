from utils.helpers import select, fit, unif_crossover, mutate, convert_binary
from utils.config import pop_size0, dna_size0, mut_rate0, dp0
import numpy as np
import matplotlib.pyplot as plt


class GA():
    def __init__(self, f, obj, domain):
        '''
        class constructor
        :param self: (class GA)
        :param f: (Function)
        :param domain: (list (Anyof Float Int) (Anyof Float Int))
        :return: (None)
        '''
        self.f = f
        self.obj = obj
        self.domain = domain

    def optimize(self,
                 m,
                 pop_size=pop_size0,
                 dna_size=dna_size0,
                 mut_rate=mut_rate0,
                 dp=dp0):
        '''
        returns a dictionary of max/min fn and argmax/argmin fn s.t domain
        is satisfied by running m times(generations)
        :param self: (class GA)
        :param m: (Nat)
        :param pop_size: (Nat)
        :param dna_size: (Nat)
        :param mut_rate: (Float)
        :return: (Dict Str Float)
        '''
        # agrs and opts are used for tracking convergence
        self.args = []
        self.opts = []
        fn = (lambda x: self.f(x) * -1) if self.obj == 'min' else self.f
        pop = np.random.randint(2, size=(pop_size, dna_size))
        for _ in range(m):
            elite_ind = np.argmax(fit(pop, fn, self.domain))
            pop = np.append(pop[elite_ind].reshape(1, -1),
                            select(pop,
                                   size=len(pop) - 1,
                                   fn=fn,
                                   domain=self.domain),
                            axis=0)
            pop_copy = pop.copy()

            for person in pop:
                kids = unif_crossover(person, pop_copy)
                np.apply_along_axis(mutate, 1, kids, mut_rate)
                kid_selected = kids[np.argmax(fit(kids, fn, self.domain))]
                person[:] = kid_selected
            opt_dna = pop[np.argmax(fit(pop, fn, self.domain))]
            arg = convert_binary(opt_dna, self.domain)
            self.args.append(round(arg, dp))
            self.opts.append(round(self.f(arg), dp))
        return {'arg': self.args[-1], 'optimum': self.opts[-1]}

    def graph(self):
        '''
        Ploted the convergence graph for args and optimum value
        :param self: (class GA)
        :return: (None)
        '''
        fig, axs = plt.subplots(2, 1, figsize=(20, 8))
        axs[0].plot(range(1, len(self.args) + 1), self.args)
        axs[1].plot(range(1, len(self.opts) + 1), self.opts)
        axs[0].set_title('Args vs iterations')
        axs[1].set_title('optimum values vs iterations')
        fig.suptitle('Convergence Plot')
        plt.show()


if __name__ == '__main__':
    ga1 = GA(lambda x: x**3 - np.exp(x), 'max', [-10, 10])
    print(ga1.optimize(50))
    ga1.graph()

    ga2 = GA(lambda x: sum((np.arange(100) * x - np.arange(100) * -2)**2),
             'min', [-10, 10])
    print(ga2.optimize(50))
    ga2.graph()
