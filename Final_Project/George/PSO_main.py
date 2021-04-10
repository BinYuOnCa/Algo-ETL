import random
import math
import numpy as np


Vector = np.array([float])

def error_msg(str):
    raise str


def cost_function(array_x: Vector=np.array([1]),
                  array_a: Vector=np.array([1]),
                  array_r: Vector=np.array([1]),
                  c: float = 0) -> float:
    """
    Function in form y = sum(a_i * (x_i ** r_i)) + c
    :param array_x: [x_0, x_1, x_2, ...]
    :param array_a: [a_0, a_1, a_2, ...]
    :param array_r: [r_0, r_1, r_2, ...]
    :param c: float number
    :return: float number
    """
    try:
        if len(array_x) == len(array_a) == len(array_r):
            cost = 0
            cost = np.sum(np.multiply(array_a, np.power(array_x, array_r))) + c
            return cost
        else:
            return None
    except:
        return None


class Particle:

    def __init__(self, dimension: int, boundary: float=float(50)):
        """

        :param dimension: int value of the dimension
        :param boundary: set boundary of all dimensions
        """
        self.boundary = boundary
        self.position = np.array([])
        self.velocity = np.array([])
        for i in range(0, dimension):
            self.position = np.append(self.position, (-1) ** (bool(random.getrandbits(1))) * random.random() * boundary)
            self.velocity = np.append(self.velocity, 0)
        self.pbest_position = self.position
        self.pbest_value = float('inf')

    def __str__(self):
        print("I am at ", self.position, " meu pbest is ", self.pbest_position, " pbest value is: ", self.pbest_value)

    def move(self):
        self.position = np.clip(self.position + self.velocity, -self.boundary, self.boundary)


class Space:

    def __init__(self, target:float, boundary:float=float(50),
                 array_a=np.array([1]), array_r=np.array([1]), c=0,
                 target_error=float(1e-6), n_particles=int(30),
                 w=0.5, c1=0.8, c2=0.9):
        """
        initialize search space.
        :param target:
        :param boundary:
        :param array_a:
        :param array_r:
        :param c:
        :param target_error:
        :param n_particles:
        :param w:
        :param c1:
        :param c2:
        """
        self.array_a = array_a
        self.array_r = array_r
        self.c = c
        self.w = w
        self.c1 = c1
        self.c2 = c2
        if len(array_a) == len(array_r):
            self.dimension = len(array_a)
        else:
            raise error_msg("function dimension not matching")
        self.boundary = boundary
        self.target = target
        self.target_error = target_error
        self.n_particles = n_particles
        self.particles = []
        self.gbest_value = float('inf')
        self.gbest_position = np.array([])
        for i in range(0, self.dimension):
            self.gbest_position = np.append(self.gbest_position, random.random() * boundary)
        self.particles_vector = [Particle(self.dimension, self.boundary) for _ in range(self.n_particles)]

    def print_particles(self):
        for particle in self.particles:
            particle.__str__()

    def fitness(self, particle):
        result = cost_function(particle.position, self.array_a, self.array_r, self.c) - self.target
        if result is not None:
            return result
        else:
            return None

    def set_pbest(self):
        for particle in self.particles:
            fitness_cadidate = self.fitness(particle)
            if fitness_cadidate is not None:
                if abs(particle.pbest_value) > abs(fitness_cadidate):
                    particle.pbest_value = fitness_cadidate
                    particle.pbest_position = particle.position

    def set_gbest(self):
        for particle in self.particles:
            best_fitness_cadidate = self.fitness(particle)
            if best_fitness_cadidate is not None:
                if (abs(self.gbest_value) > abs(best_fitness_cadidate)):
                    self.gbest_value = best_fitness_cadidate
                    self.gbest_position = particle.position

    def move_particles(self):
        for particle in self.particles:
            new_velocity = (self.w * particle.velocity) + (self.c1 * random.random()) * \
                           (particle.pbest_position - particle.position) + \
                           (random.random() * self.c2) * (self.gbest_position - particle.position)
            particle.velocity = new_velocity
            particle.move()

def validate_function(array_a, array_r, c):
    if len(array_a) == len(array_r):
        func = f"y = "
        for i in range (0, len(array_a)):
            func = func + f"{array_a[i]} * (x_{i+1} ^ {array_r[i]}) + "
        return func + f"({c})"

    else:
        return None



if __name__ == "__main__":
    print("welcome to PSO calculation, Function in form target = sum(a_i * (x_i ** r_i)) + c")

    target_error = float(1e-6)
    n_particles = int(30)
    n_iterations = int(50)

    w = 0.5
    c1 = 0.8
    c2 = 0.9

    # """
    # Function in form target = sum(a_i * (x_i ** r_i)) + c
    # :param array_x: [x_0, x_1, x_2, ...]
    # :param array_a: [a_0, a_1, a_2, ...]
    # :param array_r: [r_0, r_1, r_2, ...]
    # :param c: float number
    # """

    array_a = np.array([-12])
    array_r = np.array([1])
    c = 3
    target = 0

    boundary = 10

    if (str(input("Do you want to enter parameter manually? y/n [n]: ")).lower() or "n") == 'y':

        # """
        # Function in form target = sum(a_i * (x_i ** r_i)) + c
        # :param array_x: [x_0, x_1, x_2, ...]
        # :param array_a: [a_0, a_1, a_2, ...]
        # :param array_r: [r_0, r_1, r_2, ...]
        # :param c: float number
        # """
        target_error = float(input("acceptable error amount (eg 1e-6): " ) or "1e-6")
        n_particles = int(input("integer number of particles to simulate (eg 30): ") or "30")
        n_iterations = int(input("integer maximum number of iterations (eg 50): ") or "50")
        array_a = np.array(np.float_(str(input("comma separated string of 'a' parameter without quote: (eg 12):") or "12").split(",")))
        array_r = np.array(np.float_(str(input("comma separated string of 'r' parameter without quote: (eg 1):") or "1").split(",")))
        c = float(input("'c' parameter (eg -3): ") or "-3")
        target = float(input("target number (eg 0): ") or "0")

        boundary = float(input("boundary on all axis to calculate (eg 10): ") or "10")


    # print(cost_function(np.array([-0.25]), np.array([12]), np.array([1]), 3))
    func = validate_function(array_a, array_r, c)
    if func is None:
        print(f"Your calculating function is not valid for calculation. check you entry")
    else:
        print(f"Your calculating function appears like {func}, calculating one of solution y target is {target}")
        search_space = Space(target, boundary, array_a, array_r, c, target_error, n_particles, w, c1, c2)

        search_space.particles = search_space.particles_vector
        # search_space.print_particles()

        iteration = 0
        while (iteration < n_iterations):
            search_space.set_pbest()
            search_space.set_gbest()

            if (abs(search_space.gbest_value) <= search_space.target_error):
                break

            search_space.move_particles()
            iteration += 1

        print("The best solution is: ", search_space.gbest_position,
              " in n_iterations: ", iteration,
              " target_error: ", search_space.gbest_value)

