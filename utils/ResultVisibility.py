import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np

# 生成三维数据
x = np.array([10,50,100,200])
y = np.array([5,10,20,50,100])
x, y = np.meshgrid(x, y)
z = np.array([[-5.26,-4.88,-3.06,1.4],
              [-8.01,5.06,4.3,10.82],
              [0.2,2.38,11.38,12.07],
              [-3.11,16.95,15.17,16.34],
              [3.38,16.57,23.89,24.97]])

# 绘制三维图形
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.plot_surface(x, y, z, cmap='viridis')

plt.show()