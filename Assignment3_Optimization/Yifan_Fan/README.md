# PSO Function Introduction

## 1. Source

This function is mainly created based on two PSO models from:

1. **An improved Particle Swarm Optimization with Revivable Leaders and Its Application in Constrained NP Optimization** (*Harry Liang*)
2. **A Chaos-Enhanced Particle Swarm Optimization with Adaptive Parameters and Its Application in Maximum Power Point Tracking** (*Ying-Yi Hong*)

Please refer the source for reference at this link: https://www.hindawi.com/journals/mpe/2016/6519678/#introduction



## 2. Introduction

The mathematical equation is represented as follows:
$$
v_p^{t+1} = \chi \times \omega_{chaos}^t \times v_p^t + c_1^t \times(p_{best}^t-x_p^t) + c_2^t \times(g_{best}^t-x_p^t)
$$

$$
x_p^{t+1} = x_p^t+v_p^{t+1}
$$

Where $ \chi $ is Type 1'' constriction coefficient which is integrated to the proposed variant of PSO to prevent the divergence of the particles:
$$
\chi=\frac{2}{(\phi-2+\sqrt{\phi^2-4\phi})}
$$

$$
\phi=c_1^t+c_2^t
$$

and $ \omega_{chaos}^t $ is the chaotic random inertia weight, which is given by
$$
\omega_{chaos}^t = 0.5 \times rand(.) + 0.5 \times z_{t+1}
$$

$$
z_t+1=\left|\sin\left(\frac{\pi z_t}{rand(.)}\right) \right|
$$

$ c_1^t, c_2^t $ are time-varying cognitive and social parameters which are incorporated into PSO to improve its local and the global search by making the cognitive component large and the social component small at the initialization or in the early part of the evolutionary process.

 