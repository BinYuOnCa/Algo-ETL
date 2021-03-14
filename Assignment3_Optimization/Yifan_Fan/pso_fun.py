"""
This script is to build the related function of Particle Swarm Optimization.
"""

import numpy as np


def pso_min(func_eva, low_bd, up_bd, cons=None,
            swarm_size=100, gen_max=100, min_diff=1e-8,
            c1_init=3, c1_final=1.5, c2_init=3, c2_final=2.5,
            random_seed=None, show_result=False):
    """
    Perform a Particle Swarm Optimization (find minimum value) on the specific function.


    :param func_eva: (function) The function to be optimized
    :param low_bd: (array) The lower bounds of the input variable(s)
    :param up_bd: (array) The upper bounds of the input variable(s)
    :param cons: (function) 1-D array in which each element must be greater or equal to 0 (Default: None)
    :param swarm_size: (int) The number of particles (Default: 100)
    :param gen_max: (int) The maximum number of iterated generations (Default: 100)
    :param min_diff: (float) The minimum change of the best value to stop iteration (Default: 1e-8)
    :param c1_init: (float) the initial weight of the importance of the particle's optimized value, aka the cognitive parameter (Default: )
    :param c1_final: (float) the final value of the cognitive parameter (Default: 0.3)
    :param c2_init: (float) the initial weight of the importance of the global optimized value,
                            aka the social parameter (Default: )
    :param c2_final: (float) the final value of the social parameter (Default: 0.3)
    :param random_seed: (int) set the random set (Default: None)
    :param show_result: (logic) choose whether to show the result (Default: False)
    :return min_value, g_best: The optimum minimum value and its corresponding solution
    """
    # ===== ===== STEP 1. Check Availability ===== =====

    assert len(low_bd) == len(up_bd), "The two bounds should have the same length."
    assert hasattr(func_eva, '__call__'), "Ensure the function is callable"
    low_bd = np.array(low_bd)
    up_bd = np.array(up_bd)
    assert np.all(up_bd >= low_bd), "All upper bound cannot smaller than lower bound"

    # ===== ===== STEP 2. Define Required Functions ===== =====

    def target_fun(x):
        """
        Use sequential unconstrained minimization technique (SUMT) to build an nonlinear
        optimization function.

        :param x: (array) the input variables of function
        :return: (float) the output of the optimized function build by SUMT
        """
        if cons is None:  # no constraints
            return func_eva(x)
        else:
            con_res = np.array(cons(x))
            loss_func = 0
            # build loss function:
            for i in con_res:
                if i <= 0:
                    loss_func += 1e10
                else:
                    loss_func += 1 / i
            return func_eva(x) + loss_func

    def over_bound_handler(pre_pos, now_pos, ld, ud):
        """
        This function will map the amplified space of the particles
        which moved to outside boundary, to the inside space keeping in
        original scale.

        :param pre_pos: (array) The previous positions of particles
        :param now_pos: (array) The estimated positions of particles
        :param ld:(array) The lower bounds of the input variable(s)
        :param ud: (array) The upper bounds of the input variable(s)
        :return: (array) the adjusted positions of particles
        """
        # Check if the new position is out of bounds:
        # For the positions which out of the upper bound:
        check_up = (now_pos > ud)
        delta_up_bound = ud - pre_pos
        delta_up_range = now_pos - pre_pos
        delta_up_range = np.where(delta_up_range == 0, 1, delta_up_range)
        delta_up_pos = (delta_up_bound * delta_up_bound) / delta_up_range
        adj_up_pos = pre_pos + delta_up_pos

        # For the positions which out of the lower bound:
        check_low = (now_pos < ld)
        delta_lo_bound = pre_pos - ld
        delta_lo_range = pre_pos - now_pos
        delta_lo_range = np.where(delta_lo_range == 0, 1, delta_lo_range)
        delta_lo_pos = (delta_lo_bound * delta_lo_bound) / delta_lo_range
        adj_lo_pos = pre_pos - delta_lo_pos

        # Update the new positions
        pre_pos[check_up] = adj_up_pos[check_up]
        pre_pos[check_low] = adj_lo_pos[check_low]

        return pre_pos

    def cal_inertia_weight(z_para):
        """
        This function will create a random inertia weight.

        :param z_para: (float) the inertia parameter.
        :return: z_new (float) the chaotic random inertia weight
        """
        z_para_new = np.abs(np.sin(np.pi * z_para / np.random.uniform(0, 1)))
        return z_para_new

    def cal_cons_para(c1, c2):
        """
        This function is to create the Type 1'' constriction coefficient to prevent the
        divergence of the particles.

        :param c1: (float) current cognitive parameter, aka the weight of particle's best value
        :param c2: (float) Social parameter, aka the weight of the global best value
        :return: x_para: (float) the constriction coefficient.
        """
        c_sum = c1 + c2
        x_cons_para = 2 / (c_sum - 2 + np.sqrt((c_sum ** 2) - (4 * c_sum)))
        return x_cons_para

    # ===== ===== STEP 3. Initialization ===== =====
    np.random.seed(random_seed)
    dim = len(low_bd)
    # Initial positions:
    pos = np.random.uniform(low_bd, up_bd, (swarm_size, dim))
    # Initial velocities: (All zeros)
    vlt = np.zeros_like(pos) + np.random.uniform(0, 1)
    # Initial individual best solutions:
    ind_best = pos.copy()
    # Initial the corresponding outputs:
    p_output = np.apply_along_axis(target_fun, 1, pos)
    # Initial the global best value position:
    min_value = min(p_output)
    g_index = np.argmin(p_output)
    g_best = pos[g_index, :]

    # ===== ===== STEP 4. Main Process ===== =====
    gen_count = 1
    z_para = 0.7  # the initial inertia parameter

    while gen_count <= gen_max:
        # update the inertia weight:
        z_para = cal_inertia_weight(z_para)
        wt_inertia = 0.5 * (np.random.uniform(0, 1) + z_para)

        # Update the cognitive and social parameters:
        c_cog = c1_final - (gen_count / gen_max) * (c1_final - c1_init)
        c_soc = c2_init + (gen_count / gen_max) * (c2_final - c2_init)
        # Update the corresponding constriction coefficient:
        x_para = cal_cons_para(c_cog, c_soc)

        # Update the velocity:
        vlt = (x_para * wt_inertia * vlt) + c_cog * (ind_best - pos) + c_soc * (g_best - pos)
        # Update the particle's positions:
        new_pos = pos + vlt

        # Update the positions which in domain:
        pos = over_bound_handler(pos, new_pos, low_bd, up_bd)
        # Update the new corresponding outputs:
        new_p_output = np.apply_along_axis(target_fun, 1, pos)
        # Update the new individual best solutions:
        better_pos = new_p_output < p_output
        ind_best[better_pos, :] = pos[better_pos, :]
        p_output = new_p_output
        # Update the new global best solution:
        new_min_value = min(p_output)

        if np.abs(new_min_value - min_value) < min_diff and gen_count > (gen_max / 5):
            print("Achieve the minimum difference of {} at {} iterations.".format(min_diff, gen_count))
            break
        else:
            min_value = new_min_value
            g_best = pos[np.argmin(p_output), :]

        gen_count += 1

    if show_result:
        print("The achieved optimum (minimum) value is {}".format(round(min_value, 2)))
        print("The corresponding solution is {}".format(np.around(g_best, 2)))

    return min_value, g_best
