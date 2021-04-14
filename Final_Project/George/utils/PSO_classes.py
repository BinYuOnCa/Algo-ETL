import numpy as np
import random


W = 0.5
c1 = 0.8
c2 = 0.9


class Particle():
    def __init__(self):
        self.position = np.array([(-1) ** (bool(random.getrandbits(1))) * random.random() * 50,
                                  (-1) ** (bool(random.getrandbits(1))) * random.random() * 50])
        # self.position = np.array([(-1) ** (bool(random.getrandbits(1))) * random.random() * 2,
        #                           (-1) ** (bool(random.getrandbits(1))) * random.random() * 2])
        self.pbest_position = self.position
        self.pbest_value = float('inf')
        self.velocity = np.array([0, 0])

    def __str__(self):
        print("I am at ", self.position, " meu pbest is ", self.pbest_position)

    def move(self):
        self.position = self.position + self.velocity


class Space():

    def __init__(self, target, target_error, n_particles):
        self.target = target
        self.target_error = target_error
        self.n_particles = n_particles
        self.particles = []
        self.gbest_value = float('inf')
        self.gbest_position = np.array([random.random() * 50, random.random() * 50])
        # self.gbest_position = np.array([random.random() * 2, random.random() * 2])

    def print_particles(self):
        for particle in self.particles:
            particle.__str__()
            # pass

    def fitness(self, particle):
        return particle.position[0] ** 2 + particle.position[1] ** 2 + 1
        # return ((-1) * particle.position[0]) ** 2 + particle.position[1] ** 1 + 1

    def set_pbest(self):
        for particle in self.particles:
            fitness_cadidate = self.fitness(particle)
            if (particle.pbest_value > fitness_cadidate):
                particle.pbest_value = fitness_cadidate
                particle.pbest_position = particle.position

    def set_gbest(self):
        for particle in self.particles:
            best_fitness_cadidate = self.fitness(particle)
            if (self.gbest_value > best_fitness_cadidate):
                self.gbest_value = best_fitness_cadidate
                self.gbest_position = particle.position

    def move_particles(self):
        for particle in self.particles:
            global W
            new_velocity = (W * particle.velocity) + (c1 * random.random()) * (
                        particle.pbest_position - particle.position) + \
                           (random.random() * c2) * (self.gbest_position - particle.position)
            particle.velocity = new_velocity
            particle.move()

    def return_gbest(self):
        return self.gbest_value

